# encoding: utf-8

from setuptools import find_packages, setup

install_requires = [
    "typer",
    "rich",
    "dictdiffer",
    "loguru",
    "aiomysql",
    "motor",
    "asyncpg",
    "pandas",
    "openpyxl",
    "tqdm",
    "pydantic"
]
exclude_packages = ['*output', '*config', 'logs', 'test', '*test*', '*.yaml', '*.yml']
include_packages = ['diff_kit', 'diff_kit.*']

setup(
    name="diff_kit",
    version="0.1.5",
    description="对比工具集",
    long_description=open('README.md', encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    author="alan",
    author_email="280262569@qq.com",
    license="Apache License 2.0",
    url="https://gitee.com/Test-Alan/diff-kit",
    python_requires='>=3.8',
    packages=find_packages(exclude=exclude_packages, include=include_packages),
    package_data={},
    keywords='diff mysql db',
    install_requires=install_requires,
    classifiers=[
        "Development Status :: 3 - Alpha",
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
)


# python setup.py sdist bdist_wheel
# twine upload --repository pypi