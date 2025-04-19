from setuptools import setup, find_packages

setup(
    name="tende-api",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "fastapi",
        "uvicorn",
        "psycopg",
        "python-multipart",
        "python-jose[cryptography]",
        "passlib[bcrypt]",
        "datadog",
        "ddtrace",
    ],
    python_requires=">=3.11",
) 