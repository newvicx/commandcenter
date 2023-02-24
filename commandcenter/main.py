from hyprxa import Hyprxa
from hyprxa.application import _ADMIN_USER

import commandcenter.__version__
from commandcenter.auth.backend import ActiveDirectoryBackend
from commandcenter.routes import dialout_router
from commandcenter.sources.pi_web.models import PIWebSubscription
from commandcenter.sources.traxx.models import TraxxSubscription
from commandcenter.settings import (
    AUTH_SETTINGS,
    DIALOUT_SETTINGS,
    PIWEB_SETTINGS,
    TRAXX_SETTINGS
)



app = Hyprxa(
    title=commandcenter.__version__.__title__,
    description=commandcenter.__version__.__description__,
    version=commandcenter.__version__.__version__,
    auth_client=AUTH_SETTINGS.get_client,
    auth_backend=ActiveDirectoryBackend,
    interactive_auth=True
)


app.include_router(dialout_router)


app.add_source(
    source="pi_web",
    integration=PIWEB_SETTINGS.get_integration,
    subscription_model=PIWebSubscription,
    scopes=PIWEB_SETTINGS.scopes,
    any_=PIWEB_SETTINGS.any,
    raise_on_no_scopes=PIWEB_SETTINGS.raise_on_no_scopes
)
app.add_source(
    source="traxx",
    integration=TRAXX_SETTINGS.get_integration,
    subscription_model=TraxxSubscription,
    scopes=TRAXX_SETTINGS.scopes,
    any_=TRAXX_SETTINGS.any,
    raise_on_no_scopes=TRAXX_SETTINGS.raise_on_no_scopes
)

for scope in DIALOUT_SETTINGS.scopes:
    _ADMIN_USER.scopes.add(scope)