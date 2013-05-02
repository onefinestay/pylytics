"""
pylytics
========

"""

try:
    VERSION = __import__('pkg_resources').get_distribution('pylytics').version
except:
    VERSION = 'unknown'
