"""Run the established POC contract against the actual packaged implementation.

The original tests remain intact and executable against the standalone POC.
The substitutions below are restricted to the test fixture's imported module
references, and restored after each test.
"""
from unittest.mock import patch
from types import SimpleNamespace
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

import test_arlo_cert_poc as certificate_tests
import test_rtsp_transport as transport_tests
from custom_components.arlo_local import arlo_cert_poc, rtsp_transport


class PackagedCertificateTests(certificate_tests.TestPOC):
    def setUp(self):
        self.enterContext(patch.object(certificate_tests, "poc", arlo_cert_poc))
        super().setUp()

    def test_invalid_certificate_inputs_and_corrupt_cache(self):
        for data in ({}, {"certsData":[]}, {"certsData":[None]},
                     {"certsData":[{"peerCert":123,"deviceCert":"bad"}],"icaCert":"bad"}):
            with self.subTest(data=data),self.assertRaises((ValueError,TypeError)):
                arlo_cert_poc.validate_and_save(self.security,self.hub.unique_id,data)
        with self.assertRaises(ValueError):
            arlo_cert_poc.prepare(self.tmp.name,self.hub,request_uuid="")
        with self.assertRaises(ValueError):
            arlo_cert_poc.request_params(self.security,self.hub,"")
        mismatched=SimpleNamespace(public_key=rsa.generate_private_key(public_exponent=65537,key_size=2048).public_key().public_bytes(
            serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode(),
            private_key=self.security.private_key)
        with self.assertRaises(ValueError):
            arlo_cert_poc.request_params(mismatched,self.hub,"test")
        diagnostic=arlo_cert_poc.request_diagnostic(200,{"meta":{"code":"secret","error":-1}})
        self.assertIsNone(diagnostic["api_meta_code"])
        self.assertIsNone(diagnostic["api_meta_error"])
        arlo_cert_poc.validate_and_save(self.security,self.hub.unique_id,self._response())
        from pathlib import Path
        Path(self.security.private_key_path).write_text("invalid PEM")
        self.assertEqual(arlo_cert_poc.inspect_cache(self.root,self.hub.unique_id)["status"],"invalid")


_TRANSPORT_SYMBOLS = {
    name: getattr(rtsp_transport, name)
    for name in ("Message", "Nonce", "Relay", "read_unit", "rewrite_message",
                 "tls_context", "probe_tls", "close_writer")
}


class PackagedParserTests(transport_tests.ParserTests):
    def setUp(self):
        self.enterContext(patch.multiple(transport_tests, **_TRANSPORT_SYMBOLS))
        super().setUp()


class PackagedTLSTests(transport_tests.TLSTests):
    async def asyncSetUp(self):
        self.enterContext(patch.multiple(transport_tests, **_TRANSPORT_SYMBOLS))
        await super().asyncSetUp()

    async def test_missing_partial_chain_support_fails_closed(self):
        import ssl
        from pathlib import Path
        hub=Path(self.security.device_certs_path('test-hub'))
        unsupported=SimpleNamespace(PEM_cert_to_DER_cert=ssl.PEM_cert_to_DER_cert,
            SSLContext=ssl.SSLContext,PROTOCOL_TLS_CLIENT=ssl.PROTOCOL_TLS_CLIENT,
            CERT_REQUIRED=ssl.CERT_REQUIRED)
        with patch.object(rtsp_transport,'ssl',unsupported):
            with self.assertRaisesRegex(RuntimeError,'partial-chain trust'):
                rtsp_transport.tls_context(peer_chain=hub/'combined.crt',
                    private_key=Path(self.security.private_key_path),device_cert=hub/'device.crt',
                    ica_cert=Path(self.security.certs_path)/'ica.crt')
