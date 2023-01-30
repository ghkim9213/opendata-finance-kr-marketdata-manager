from pathlib import Path
import datetime
import os

def read_secret(secret_name):
    with open(f'.secrets/{secret_name}', 'r') as f:
        secret = f.read().strip()
    return secret


BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = read_secret('MARKETDATA_MANAGER_SECRET_KEY')

DEBUG = False

ALLOWED_HOSTS = []

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'api',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'config.urls'

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'marketdata',
        'USER': 'admin',
        'PASSWORD': read_secret('AWS_RDS_PASSWORD'),
        'HOST': 'opendata-finance-kr-marketdata.cai2wlj5r9yu.ap-northeast-2.rds.amazonaws.com',
        'PORT': '3306',
        'OPTIONS': {
            'init_command': "SET sql_mode='STRICT_TRANS_TABLES'",
        }
    }
}

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'Asia/Seoul'

USE_I18N = True

USE_TZ = False

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# s3
AWS_ACCESS_KEY_ID = read_secret('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = read_secret('AWS_SECRET_ACCESS_KEY')
DEFAULT_FILE_STORAGE = 'storages.backends.s3boto3.S3Boto3Storage'
AWS_STORAGE_BUCKET_NAME = 'opendata-finance-kr'


# sources
OPENAPI_SERVICE_KEY_DECODED = read_secret('OPENAPI_SERVICE_KEY_DECODED')
OPENAPI_SERVICE_KEY_ENCODED = read_secret('OPENAPI_SERVICE_KEY_ENCODED')
OPENAPI_DATA_STARTS_ON = datetime.date(year=2020, month=1, day=2)

OPENDART_SERVICE_KEY = read_secret('OPENDART_SERVICE_KEY')


# products
PORTFOLIO_DATA_STARTS_ON = datetime.date(year=2022, month=12, day=29)


#########################
# Although batch manager app does not utilize django frontend properties,
# we leave following default configurations to use admin page in development.

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


STATIC_URL = 'static/'


TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]
