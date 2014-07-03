PYLYTICS_DB = "test_warehouse"

DATABASES = {
    "test_warehouse": {
        "host": "localhost",
        "user": "root",
        "passwd": "",
        "db": "test_warehouse",
    },
    "middle_earth": {
        "host": "localhost",
        "user": "root",
        "passwd": "",
        "db": "middle_earth",
    },
}

# Only used for pylytics.plugins.nameko
NAMEKO_AMQP_URI = 'amqp://guest:guest@localhost:5672//'

# A Sentry DSN configured to receive log output.
SENTRY_DSN = 'http://public:secret@example.com/1'
