from pylytics.library.utils import _camel_to_snake, _camel_to_title_case


def test_camel_to_snake():
    assert _camel_to_snake('HelloWorld') == 'hello_world'


def test_camel_to_title_case():
    assert _camel_to_title_case('HelloWorld') == 'Hello World'
