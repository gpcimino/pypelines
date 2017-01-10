import setuptools
if __name__ == '__main__':
    setuptools.setup(
        name='pypelines',
        version='0.1',

        # This automatically detects the packages in the specified
        # (or current directory if no directory is given).
        packages=setuptools.find_packages(),

        zip_safe=False,

        author='Giampaolo Cimino',
        author_email='giampaolo.cimino@cmre.nato.int',

        description='Simple Python framework to create pipelines systems',

        # For this parameter I would recommend including the
        # README.rst
        long_description='''With this framework the user can create simple pipelines system (a.k.a. workflows) to quickly setup Python processes for data acquisition, ETL, data conversion, data analysis, web data scraping.
        The User can define each stage of the process in a single block and then connect block together via synchronous or asyncrhronous pipes. 
        User can beat the GIL exploiting the multiprocess capabilities provided by pypelines.''',

        # The license should be one of the standard open source
        # licenses: https://opensource.org/licenses/alphabetical
        license='MIT',

        # Homepage url for the package
        #url='http://...',

        install_requires=[
          'Jinja2',
          'CherryPy',
          'pickleablelambda',
        ],

        dependency_links=['https://github.com/gpcimino/pickleablelambda/archive/master.zip#egg=pickleablelambda-0.1.0']

    )