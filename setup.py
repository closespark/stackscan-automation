#!/usr/bin/env python3
"""Setup script for Tech Stack Scanner."""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

with open("requirements.txt", "r", encoding="utf-8") as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith("#")]

setup(
    name="stackscan-automation",
    version="1.0.0",
    author="Stack Scanner Team",
    description="Detect technology stacks on websites and generate personalized outreach emails",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/closespark/stackscan-automation",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.10",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "stack-scanner=stackscanner.cli:main",
            "tech-scanner=stackscanner.tech_cli:main",
        ],
    },
)
