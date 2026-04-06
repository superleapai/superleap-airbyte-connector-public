from setuptools import setup, find_packages

setup(
    name="source_superleap_crm",
    version="0.1.0",
    description="Airbyte source connector for Superleap CRM",
    author="Superleap",
    packages=find_packages(),
    install_requires=[
        "airbyte-cdk",
        "requests",
    ],
    entry_points={
        "console_scripts": [
            "source-superleap-crm=main:main",
        ],
    },
)
