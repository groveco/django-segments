SECRET_KEY = 'shhh'
SITE_ID = 1

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': ':memory:',
    },
    'other': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': ':memory:',
    }
}

DATABASE_ROUTERS = ['segments.router.SegmentsRouter',]

ROOT_URLCONF = 'segments.tests.urls'

AUTH_USER_MODEL = 'tests.SegmentableUser'

TEMPLATE_CONTEXT_PROCESSORS = (
    "django.core.context_processors.static",
    "django.core.context_processors.request",
)

TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.Loader',
    'django.template.loaders.app_directories.Loader',
)

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.sites',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django.contrib.admin',
    'segments',
    'south',
    'segments.tests',
    'celery'
)

AUTHENTICATION_BACKENDS = (
    "django.contrib.auth.backends.ModelBackend",
)

STATIC_URL = '/static/'