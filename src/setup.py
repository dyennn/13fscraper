from setuptools import setup, find_packages

setup(
    name="thirteenf-scraper",
    version="0.1.0",
    description="13F Scraper: Scrape, analyze, and export 13f.info filings.",
    author="Your Name",
    packages=find_packages(),
    install_requires=[
        "requests",
        "beautifulsoup4",
        "tqdm",
        "tabulate",
        "pyyaml",
        "python-dotenv"
    ],
    entry_points={
        'console_scripts': [
            'thirteenf-scraper = v4.v4.1_scraper:main',
            'thirteenf-analysis = v4.analysis:menu',
        ],
    },
    python_requires='>=3.7',
    include_package_data=True,
    package_data={"": ["config.yaml"]},
)
