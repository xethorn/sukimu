from setuptools import find_packages
from setuptools import setup


setup(
    name='Sukimu',
    version='2.0.5',
    url='https://github.com/xethorn/sukimu',
    author='Michael Ortali',
    author_email='github@xethorn.net',
    description=(
        'Standardized way to perform CRUD operations with Field validation'),
    license='MIT',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        'Development Status :: Alpha',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],)
