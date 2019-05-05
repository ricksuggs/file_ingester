from setuptools import setup

setup(
    name="file_ingester",
    packages=["app"],
    install_requires=[],
    setup_requires=["pytest-runner"],
    tests_require=["pytest", "pytest_mockito"],
)
