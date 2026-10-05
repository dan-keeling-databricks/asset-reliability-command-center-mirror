import pytest

from backend.identity import LocalDatabricksIdentityProvider


class StubConfig:
    def __init__(self, headers):
        self.headers = headers

    def authenticate(self):
        return self.headers


class StubWorkspaceClient:
    def __init__(self, headers):
        self.config = StubConfig(headers)


async def test_local_identity_extracts_cli_bearer_token() -> None:
    provider = LocalDatabricksIdentityProvider(
        workspace_client=StubWorkspaceClient({"Authorization": "Bearer local-token"})
    )
    assert await provider.token() == "local-token"


async def test_local_identity_rejects_missing_bearer_token() -> None:
    provider = LocalDatabricksIdentityProvider(workspace_client=StubWorkspaceClient({}))
    with pytest.raises(RuntimeError, match="did not provide a bearer token"):
        await provider.token()
