from thoa.core.job_status import JobStatus, TERMINAL_STATUSES


def test_crashed_is_a_terminal_status():
    assert JobStatus("crashed") in TERMINAL_STATUSES
