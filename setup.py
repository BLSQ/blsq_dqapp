
import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="blsq_dqapp",
    version="0.0.1",
    author="FernandoVB-Bluesquare",
    author_email="fvaldesbango@bluesquarehub.com",
    description="Base code package for data auctioning analysis",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/BLSQ/blsq_dqapp",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)