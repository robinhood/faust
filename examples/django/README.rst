Directory Layout

- ``proj/``

  This is the main Django project.

  We have also added a ``proj/__main__.py`` that executes if you do
  ``python -m proj``, and it will work as the manage.py for the project.

  This is also installed by setup.py as an entrypoint, so after
  ``python setup.py install`` or ``python setup.py develop`` the
  ``proj`` command will be available::

        $ python setup.py develop
        $ proj runserver

    The above is the same as running ``manage.py runserver``, but it will
    be installed in the system path.

- ``faustapp/``

    This is a Django App that defines the Faust app used by the project,
    and it also configures Faust using Django settings.

    This faustapp is also installed by setup.py as the ``proj-faust`` program,
    and can be used to start a Faust worker for your Django project by doing::

        $ python setup.py develop
        $ proj-faust worker -l info

- ``accounts/``

    This is an example Django App with stream processors.
