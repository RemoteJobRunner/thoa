from thoa.core.api_utils import ErrorReadouts


def test_426_readout_displays_server_message(capsys):
    detail = "Your thoa CLI is outdated (0.1.3). Minimum required: 0.2.0. Run: pip install 'thoa>=0.2.0' to continue"
    ErrorReadouts(426, detail).readout()
    # Normalize Rich's terminal line-wrapping so substring asserts are robust.
    out = " ".join(capsys.readouterr().out.split())
    assert "426 Upgrade Required" in out
    # The server-provided detail (with the exact required version) must flow through
    # to the user verbatim; CLI no longer adds a duplicate hint of its own.
    assert "Minimum required: 0.2.0" in out
    assert "pip install 'thoa>=0.2.0' to continue" in out
