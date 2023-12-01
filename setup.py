from setuptools import setup, find_packages

setup(
    name='multyscan',
    version='0.11.2',
    packages=find_packages(),
    description='My custom package',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Andrei',
    author_email='andrey.shivalin@gmail.com',
    url='https://github.com/VaitaR/multyscan',
    # Если есть зависимости
    install_requires=[
        'asyncio',
        'aiohttp',
        'web3',
    ]
)
