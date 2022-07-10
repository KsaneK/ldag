from distutils.core import setup

from setuptools import find_packages

INSTALL_REQUIRES = [
    "pika~=1.3"
]
DEV_INSTALL_REQUIRES = []

setup(
    name="LDAG",
    description="Light Directed Acyclic Graph",
    author="Lukasz Olender",
    author_email="olender.lukasz96@gmail.com",
    packages=find_packages(),
    install_requires=INSTALL_REQUIRES,
    extra_require={"dev": DEV_INSTALL_REQUIRES}
)
