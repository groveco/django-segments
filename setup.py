import os
from setuptools import setup
from segments import __version__

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "django-segments",
    version = __version__,
    author = "Chris Clark, Kate Kligman",
    author_email = "chris@untrod.com",
    description = ("Create arbitrary groups of users via SQL queries."),
    license = "MIT",
    keywords = "django segments queries segmentation marketing groups sql",
    url = "https://github.com/groveco/django-segments",
    packages=['segments'],
    long_description=read('readme.rst'),
    classifiers=[
        "Topic :: Utilities",
    ],
    install_requires=[
        'Django>=1.11.6',
    ],
    include_package_data=True,
    zip_safe = False,
)
