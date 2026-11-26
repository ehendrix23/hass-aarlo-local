"""Reproducible quality gates for the isolated HA development project."""
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).resolve().parent.parent
TESTS=[
    "test_arlo_cert_poc",
    "test_rtsp_transport",
    "test_ffmpeg_transport.DecoderTest.test_ffmpeg_real_decode",
    "test_packaged_poc",
    "test_arlo_local_inventory",
    "test_arlo_local_owner_session",
    "test_arlo_local_distribution.SharedStreamTests.test_two_consumers_one_hub_connection",
    "test_arlo_local_distribution.SharedStreamTests.test_normalizer_preserves_variable_rate_source_frames",
    "test_arlo_local_distribution.SharedStreamTests.test_repeated_upstream_loss_fences_actual_decoder",
    "test_arlo_local_distribution.SharedStreamTests.test_native_http_and_rtsp_share_producer_and_generation",
    "test_arlo_local_distribution.SharedStreamTests.test_external_go2rtc_http_consumer_after_delayed_start",
    "test_arlo_local_distribution.SharedStreamTests.test_warm_lease_rollover_and_release",
    "test_arlo_local_distribution.SharedStreamTests.test_real_demand_reclaims_only_warm_only_capacity",
    "test_arlo_local_stream_fence",
    "test_arlo_local_process_owner",
    "test_arlo_local_retry",
    "test_arlo_local_budget",
    "test_arlo_local_native_video",
    "test_arlo_local_video_timeline",
    "test_arlo_local_native.NativeRuntimeTests.test_native_entities_shared_snapshot_stream_and_inventory",
    "test_arlo_local_config_flow",
    "test_arlo_local_diagnostics",
    "test_arlo_local_mfa",
    "test_arlo_local_lifecycle.LifecycleTests.test_native_entry_registry_snapshot_reload_and_unload",
    "test_arlo_local_storage",
    "test_arlo_local_warm_retry",
    "test_arlo_local_runtime_faults",
    "test_arlo_local_distribution_faults.DistributorFaultTests",
    "test_arlo_local_distribution_faults.DistributionAdmissionTests.test_hub_limit_releases_capacity_without_disrupting_active_relays",
    "test_arlo_local_distribution_faults.DistributionAdmissionTests.test_five_slot_budget_resize_and_recovery",
    "test_arlo_local_distribution_faults.DistributionAdmissionTests.test_upstream_disconnect_reconnects_only_the_failed_camera",
    "test_arlo_local_distribution_faults.DistributionAdmissionTests.test_timeline_fault_releases_only_affected_generation",
    "test_arlo_local_distribution_faults.DistributionAdmissionTests.test_active_relay_close_terminates_client_before_waiting_for_server",
    "test_arlo_local_distribution_faults.DistributionAdmissionTests.test_relay_close_during_cleanup_still_closes_local_client",
    "test_arlo_local_camera_faults",
    "test_arlo_local_protocol_faults.ProtocolFaultTests",
    "test_arlo_local_protocol_faults.BootstrapFaultTests.test_bootstrap_success_cseq_mapping_and_feedback",
    "test_arlo_local_protocol_faults.BootstrapFaultTests.test_bootstrap_failure_closes_connection",
    "test_arlo_local_protocol_faults.BootstrapFaultTests.test_refused_startup_has_bounded_retry_cooldown",
    "test_arlo_local_protocol_faults.BootstrapFaultTests.test_invalid_initial_or_subsequent_cseq",
    "test_arlo_local_protocol_faults.BootstrapFaultTests.test_invalid_first_packet_does_not_connect_and_cancel_releases",
]

def main():
    reports=ROOT/"reports"
    reports.mkdir(exist_ok=True)
    env=dict(os.environ,PYTHONPATH=".:tests",ARLO_POC_GO2RTC=str(ROOT/"ha-test/bin/go2rtc"),
             ARLO_POC_FFMPEG=str(ROOT/"ha-test/bin/ffmpeg"),
             COVERAGE_FILE=str(reports/".coverage"),
             PATH=str(ROOT/"ha-test/bin")+":"+os.environ["PATH"])
    command=[sys.executable,"-m","coverage","run","--branch","--source=custom_components.arlo_local","-m","unittest",*TESTS]
    result=subprocess.run(command,cwd=ROOT,env=env,capture_output=True,text=True,timeout=180)
    (reports/"regression.log").write_text(result.stdout+"\n"+result.stderr)
    print("\n".join(result.stderr.splitlines()[-15:]))
    if result.returncode:raise SystemExit(result.returncode)
    subprocess.run([sys.executable,"-m","coverage","json","-o",str(reports/"coverage.json")],cwd=ROOT,env=env,check=True)
    data=json.loads((reports/"coverage.json").read_text())
    for file,record in data["files"].items():
        s=record["summary"]
        b=s["num_branches"]
        print(Path(file).name, "statements",s["covered_lines"],"/",s["num_statements"],
              "branches",s["covered_branches"],"/",b,"branch %",round(s["covered_branches"]/b*100,1) if b else 100)
    s=data["totals"]
    print("TOTAL",s["percent_covered_display"],"% combined;",round(s["covered_branches"]/s["num_branches"]*100,2),"% branch")
    failures=[]
    for file,record in data['files'].items():
        summary=record['summary']
        branches=summary['num_branches']
        if branches and summary['covered_branches']*100 < branches*95:
            failures.append(f'{file}: branch coverage below 95%')
        if Path(file).name=='config_flow.py' and (summary['missing_lines'] or summary['missing_branches']):
            failures.append(f'{file}: incomplete config-flow coverage')
    if failures:
        raise SystemExit('\n'.join(failures))

if __name__=="__main__":
    main()
