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
NAMEKO_AMQP_URI = 'amqp://pylytics:pylytics@localhost:5672/pylytics'
