from setuptools import setup, find_packages

setup(
    name="pharma-insights-tool",
    version="0.1.0",
    description="Product-centric literature intelligence for pharma",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.10",
    install_requires=[
        "requests",
        "pydantic",
        "python-dotenv",
        "spacy",
        "scispacy",
    ],
)