from app.auth_service import is_non_dev_env, is_weak_jwt_secret, should_seed_default_users


def test_is_non_dev_env():
    assert is_non_dev_env("prod") is True
    assert is_non_dev_env("staging") is True
    assert is_non_dev_env("dev") is False
    assert is_non_dev_env("local") is False
    assert is_non_dev_env("test") is False


def test_is_weak_jwt_secret():
    assert is_weak_jwt_secret("dev-change-me") is True
    assert is_weak_jwt_secret("change-me") is True
    assert is_weak_jwt_secret("short") is True
    assert is_weak_jwt_secret("this-is-a-strong-secret-value") is False


def test_should_seed_default_users():
    assert should_seed_default_users("dev", None) is True
    assert should_seed_default_users("test", None) is True
    assert should_seed_default_users("prod", None) is False
    assert should_seed_default_users("prod", "true") is True
    assert should_seed_default_users("dev", "false") is False
