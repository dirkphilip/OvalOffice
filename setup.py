# -*- coding: utf-8 -*-

from setuptools import setup

setup(
        name="oval_office_2",
        version="0.1",
        py_packages=["oval_office_2"],
        install_requires=[
            "Click",
            "paramiko",
            "obspy",
            'wrapt',
            'boltons',
            'futures', 'ipyparallel', 'ipyparallel'
        ],
        entry_points="""
        [console_scripts]
        oo_2=oval_office_2.oval_office_2:cli
        """
)
