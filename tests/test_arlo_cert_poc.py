"""Offline tests using exact upstream SecurityUtils; no actual Arlo requests."""
import base64
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from pyaarlo.security_utils import SecurityUtils
from custom_components.arlo_local import arlo_cert_poc as poc


class Backend:
    def __init__(self, response=(401, None)):
        self.response, self.calls = response, []
    def _request_tuple(self, path, *, method, params, headers, raw, timeout, retry_401):
        self.calls.append((path, method, params, headers, raw, timeout, retry_401))
        return self.response


class TestPOC(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.hub = SimpleNamespace(unique_id="hub-test", device_id="test-device", xcloud_id="test-route")
        self.root, self.security, self.params, self.headers = poc.prepare(
            self.tmp.name, self.hub, request_uuid="test-client")
        self.ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "synthetic CA")])
        self.now = datetime.now(timezone.utc)
    def _cert(self, key, *, expired=False):
        return (x509.CertificateBuilder().subject_name(self.ca_name)
                .issuer_name(self.ca_name).public_key(key).serial_number(x509.random_serial_number())
                .not_valid_before(self.now-timedelta(days=3))
                .not_valid_after(self.now+timedelta(days=-1 if expired else 3))
                .sign(self.ca_key, hashes.SHA256()))
    def _response(self, mismatch=False, expired=False):
        public = serialization.load_pem_public_key(self.security.public_key.encode())
        def b64(c): return base64.b64encode(c.public_bytes(serialization.Encoding.DER)).decode()
        if mismatch: public = self.ca_key.public_key()
        return {"certsData":[{"peerCert":b64(self._cert(public, expired=expired)),
                               "deviceCert":b64(self._cert(self.ca_key.public_key()))}],
                "icaCert":b64(self._cert(self.ca_key.public_key()))}
    def test_exact_pyaarlo_reuse(self):
        self.assertIsInstance(self.security, SecurityUtils)
        self.assertEqual(Path(self.security.private_key_path).stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.root.stat().st_mode & 0o777, 0o700)
        expected = "".join(self.security.public_key.splitlines()[1:-1])
        self.assertEqual(self.params["publicKey"], expected)
        self.assertEqual(self.params["uniqueIds"], [self.hub.unique_id])
        self.assertNotIn("PRIVATE", str(self.params))
    def test_read_only_missing_cache(self):
        missing = Path(self.tmp.name)/"absent"
        self.assertEqual(poc.inspect_cache(missing,self.hub.unique_id)["status"],"missing")
        self.assertFalse(missing.exists())
    def test_default_no_request(self):
        b=Backend()
        with self.assertRaises(PermissionError):poc.request_once(b,self.params,self.headers)
        self.assertEqual(b.calls,[])
    def test_401_single_call_no_retry(self):
        b=Backend()
        with self.assertRaises(RuntimeError):
            poc.request_once(b,self.params,self.headers,issuance_authorized=True)
        self.assertEqual(len(b.calls),1)
        self.assertEqual(b.calls[0][-1],False)
    def test_application_failure_no_retry(self):
        b=Backend((200,{"success":False}))
        with self.assertRaises(RuntimeError):
            poc.request_once(b,self.params,self.headers,issuance_authorized=True)
        self.assertEqual(len(b.calls),1)
    def test_success_and_cache_reuse(self):
        data=self._response()
        b=Backend((200,{"success":True,"data":data}))
        result=poc.request_once(b,self.params,self.headers,issuance_authorized=True)
        report=poc.validate_and_save(self.security,self.hub.unique_id,result)
        self.assertEqual(report["status"],"locally_usable_candidate")
        cached=SecurityUtils(str(self.root))
        self.assertEqual(cached.private_key,self.security.private_key)
        self.assertEqual(poc.inspect_cache(self.root,self.hub.unique_id)["status"],"locally_usable_candidate")
        self.assertEqual(len(b.calls),1)
        with self.assertRaises(FileExistsError):
            poc.validate_and_save(cached,self.hub.unique_id,data)
    def test_meta_success(self):
        data=self._response()
        b=Backend((200,{"meta":{"code":200},"data":data}))
        self.assertEqual(poc.request_once(b,self.params,self.headers,issuance_authorized=True),data)
    def test_mismatched_cert_rejected(self):
        with self.assertRaises(ValueError):
            poc.validate_and_save(self.security,self.hub.unique_id,self._response(mismatch=True))
        self.assertFalse(self.security.has_device_certs(self.hub.unique_id))
    def test_expired_cert_rejected(self):
        with self.assertRaises(ValueError):
            poc.validate_and_save(self.security,self.hub.unique_id,self._response(expired=True))
        self.assertFalse(self.security.has_device_certs(self.hub.unique_id))
    def test_unsupported_backend_fails_closed(self):
        class OldBackend:
            def _request_tuple(self,path):raise AssertionError("must not call")
        with self.assertRaises(RuntimeError):
            poc.request_once(OldBackend(),self.params,self.headers,issuance_authorized=True)
    def test_failed_response_diagnostic_is_allowlisted(self):
        b=Backend((200,{"meta":{"code":"401","error":9204,"message":"SECRET_TOKEN"},
                        "data":{"certificate":"SECRET_CERT"},"message":"SECRET"}))
        with self.assertRaises(poc.CertificateRequestError) as raised:
            poc.request_once(b,self.params,self.headers,issuance_authorized=True)
        d=raised.exception.diagnostic
        self.assertEqual(d,{"backend_status":200,"body_kind":"object","api_meta_code":401,
                           "api_meta_error":9204,"api_success":None,"data_object":True})
        self.assertNotIn("SECRET",str(d))
        self.assertEqual(len(b.calls),1)
        self.assertFalse(b.calls[0][-1])
    def test_backend_error_diagnostic_preserves_ambiguity(self):
        b=Backend((500,None))
        with self.assertRaises(poc.CertificateRequestError) as raised:
            poc.request_once(b,self.params,self.headers,issuance_authorized=True)
        self.assertEqual(raised.exception.diagnostic["backend_status"],500)
        self.assertEqual(raised.exception.diagnostic["body_kind"],"absent")
        self.assertIsNone(raised.exception.diagnostic["api_success"])
        self.assertEqual(len(b.calls),1)
    def test_invalid_hub_id(self):
        with self.assertRaises(ValueError):poc.inspect_cache(self.root,"../escape")

if __name__=="__main__":
    unittest.main(verbosity=2)
