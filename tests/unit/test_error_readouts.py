from thoa.core.api_utils import ErrorReadouts


def test_426_readout_mentions_upgrade(capsys):
    ErrorReadouts(426, "Your thoa CLI is outdated (0.1.3). Minimum required: 0.1.4. Run: pip install -U thoa").readout()
    out = capsys.readouterr().out
    assert "426" in out or "Upgrade" in out or "outdated" in out.lower()
    assert "pip install -U thoa" in out
