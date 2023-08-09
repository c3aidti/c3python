"""
This bootstrap script is in alpha. It should never be publicly released. Its complexity means it
will require frequent updates and might only work with certain versions of server.
"""

from getpass import getpass
import base64
import hashlib
import io
import json
import os
import sys
import time
import warnings
from types import ModuleType
import zipfile
from threading import Thread
import inspect

IS_PY3 = sys.version_info[0] == 3


if IS_PY3:
    from http.cookies import SimpleCookie
    from urllib.parse import urlparse, urlunparse, urljoin
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError, URLError
    basestring = str
    _JSONDecodeError = json.decoder.JSONDecodeError
else:
    from urlparse import urlparse, urlunparse, urljoin
    from urllib2 import urlopen, Request, HTTPError, URLError
    from Cookie import SimpleCookie
    _JSONDecodeError = ValueError


def _is_callable_str(s):
    return (callable(s) and _is_str(s())) or _is_str(s)


def _is_none_or_callable_str(s):
    return s is None or _is_callable_str(s)


def _is_str(s):
    return s and isinstance(s, basestring)


def _is_none_or_str(s):
    return s is None or _is_str(s)


__copyright__ = "Copyright 2009-2018 C3 IoT, Inc. All Rights Reserved."

__builtin_help__ = None
__type_sys__ = None

def _c3_help_thin(subject=None, contentType=None, fetchDoc=False):
    """
    This method extends the behavior of the built-in python help() function by first checking
    if the help subject object is a C3 Type, an instance of a C3 Type, or a C3 Type method. If this is the case
    it will display the content or return it as a result.
    If this is not the case, will fallback to the default built-in implementation of help()
    Note: this method only works with "thin" type system. It should be deleted once we fully deprecate thin mode PLAT-19120.

    :param contentType: the MIME type for the rendered help doc, currently supported are: "text/html" (default for interactive mode), "text/markdown" (default non-interactive mode), or "text/plain".
    :param fetchDoc: a flag, which if set to true, will return the doc rather than rendering it. Default is to render (False).
    :return: the help doc content, or None in case the doc is rendered.
    """
    if subject is None:
        __builtin_help__()
        return

    c3_type_name = c3_method_name = None
    if type(subject).__name__ in ['method', 'instancemethod']:
        c3_method_name = subject.__name__
        obj = subject.__self__
    else:
        obj = subject
    if "TypeProxy" in [b.__name__ for b in type(obj).__bases__]:
        # This is a C3 Type.
        c3_type_name = type(obj).__name__
    elif "Obj" == type(obj).__name__:
        # This is a C3 Type instance.
        c3_type_name = obj.type

    if c3_type_name is not None:
        # Infer contentType if not specified:
        if contentType is None:
            try:
                # Is ipynb notebook interactive mode?
                if get_ipython().__class__.__name__ == "ZMQInteractiveShell": contentType = "text/html"
            except: pass
            finally:
                # Ipynb terminal interactive mode or other:
                if contentType is None: contentType = "text/markdown"

        renderSpec = __type_sys__.DocumentationRenderSpec(contentType=contentType)
        if c3_method_name is None:
            doc = base64.b64decode(__type_sys__.DocumentationRenderer.renderType(
                typeName=c3_type_name, spec=renderSpec).contents)
        else:
            doc = base64.b64decode(__type_sys__.DocumentationRenderer.renderTypeField(
                typeName=c3_type_name, fieldName=c3_method_name, spec=renderSpec).contents)
        doc = doc.decode("utf-8")

        if fetchDoc:
            return doc
        try:
            from IPython.core.display import display, HTML
            display(HTML(doc))
        except ImportError:
            print(doc)

    else:
        __builtin_help__(obj)



def _c3_help(subject=None, content_type=None, fetch_doc=False):
    """
    This method extends the behavior of the built-in python help() function by first checking
    if the help subject object is a C3 Type, an instance of a C3 Type, or a C3 Type method. If this is the case
    it will display the server-rendered documentation in HTML format or return it as a result.
    If this is not the case, the function will fallback to the built-in implementation of help()
    :param content_type: the MIME type for the rendered help doc, currently supported are: "text/html" (default for interactive mode), "text/markdown" (default non-interactive mode), or "text/plain".
    :param fetch_doc: a flag, which if set to true, will return the doc rather than rendering it. Default is to render (False).
    :return: the help doc content, or None in case the doc is rendered.
    """

    if subject is None:
        __builtin_help__()
        return

    c3_type_name = c3_method_name = None
    if type(subject).__name__ in ['method', 'instancemethod']:
        c3_method_name = subject.__name__
        obj = subject.__self__
    else:
        obj = subject
    if inspect.isclass(obj) and type(obj).__name__ == 'Type':
        # This is a C3 Type.
        c3_type_name = obj.__name__
    elif type(obj.__class__).__name__ == 'Type':
        # This is a C3 Type instance.
        c3_type_name = type(obj).__name__
    if c3_type_name is not None:
        # Infer contentType if not specified:
        if content_type is None:
            try:
                # Is ipynb notebook interactive mode?
                if get_ipython().__class__.__name__ == "ZMQInteractiveShell":
                    content_type = "text/html"
            except: pass
            finally:
                # Ipynb terminal interactive mode or other:
                if content_type is None:
                    content_type = "text/markdown"

        render_spec = __type_sys__.DocumentationRenderSpec(contentType=content_type)
        if c3_method_name is None:
            doc = __type_sys__.DocumentationRenderer.renderType(typeName=c3_type_name, spec=render_spec)
        else:
            doc = __type_sys__.DocumentationRenderer.renderTypeField(
                typeName=c3_type_name, fieldName=c3_method_name, spec=render_spec)
        contents = doc.contents.decode("utf-8")

        if fetch_doc:
            return contents
        try:
            from IPython.core.display import display, HTML
            display(HTML(contents))
        except ImportError:
            print(contents)

    else:
        __builtin_help__(obj)


class C3RemoteLoader(object):
    """
    This class implements an import hook for loading c3.py.zip remote modules into memory.
    More information on how it works you can find on dev page: https://www.python.org/dev/peps/pep-0302/
    or by reading comments below.
    """

    class Mode(object):
        REST = 'rest'
        THIN = 'thin'
        THICK = 'thick'

    VALID_MODES = (Mode.REST, Mode.THIN, Mode.THICK)
    COOKIE_HEADER_KEY = 'Set-Cookie'
    USERNAME_PROMPT = 'Username: '
    PASSWORD_PROMPT = 'Password: '

    # Allow override for testing purposes
    _user_input = input if IS_PY3 else raw_input
    _getpass = staticmethod(getpass)  # Needed for python 2 only

    def __init__(self, url=None, tenant=None, tag=None, auth=None, mode=Mode.THIN, action_id=None, define_types=True):
        # TODO Python decorators would be a great way to handle this validation logic
        to_validate = [
            (_is_callable_str, url, 'url'),
            (_is_none_or_str, tenant, 'tenant'),
            (_is_none_or_str, tag, 'tag'),
            (_is_none_or_callable_str, auth, 'auth'),
            (_is_str, mode, 'mode')
        ]

        for validate, val, name in to_validate:
            if not validate(val):
                raise ValueError(
                    'Must specify {}={}'.format(name, '"STRING"' if validate is _is_str else 'None | "STRING"'))

        if mode not in C3RemoteLoader.VALID_MODES:
            raise ValueError('Only valid modes are {} but gave {}'.format(C3RemoteLoader.VALID_MODES, mode))

        if callable(url):
            self._url_fn = url
            self._c3_server_url = self._parsed_url(self._url_fn())
        else:
            self._url_fn = None
            self._c3_server_url = self._parsed_url(url)

        auth = self._authenticate() if auth is None else auth
        if callable(auth):
            self._auth_fn = auth
            self._auth = self._auth_fn()
        else:
            self._auth_fn = None
            self._auth = auth
        self._tenant = tenant
        self._tag = tag
        self._mode = mode
        self._define_types = define_types
        self._thin_and_define_types = self._define_types and (self._mode == C3RemoteLoader.Mode.THIN)

        # Action id is required for python actions. It's used along with tokens in authorization process
        self._action_id = action_id

        self._check_tenant_tag()

        # Zip file
        self._zip_file = None

        # Extracted zip file
        self._extracted_zip_file = None

        # C3 Server metadata used in thick client as string
        self._metadata_file = None

        # C3 Server metadata as parsed json
        self._metadata = None

        # Create packages prefix to distinguish different packages, based on zip checksum
        self.package_prefix = None

        # Creating set of files that exists in the package
        # to determine whether we should use this loader for loading packages
        self._paths = None

        # Any exceptions that can be thrown during download process
        self._download_error = None

        self._load()

    def _parsed_url(self, url):
        # Parse url to validate it. This replicated logic in ServerConnection.py, should try to be more DRY
        # this is a workaround for url "stage-paas.c3-e.com". urlparse will recognize it as a path instead of netloc
        url_with_scheme = url if url[:4].lower() == 'http' else 'https://' + url
        parsed_url = urlparse(url_with_scheme)
        supported_schemes = ('http', 'https')
        scheme = parsed_url.scheme
        if scheme not in supported_schemes:
            raise ValueError('Url "{}" is malformed.'.format(url))
        return urlunparse((scheme, parsed_url.netloc, '', '', '', ''))

    def _check_tenant_tag(self):
        if self._tenant and self._tag:
            return
        res = json.loads(self._request('/api/1/Console?action=init'))
        self._tenant = res['tenant']
        self._tag = res['tag']

    def _authenticate(self):
        user = C3RemoteLoader._user_input(C3RemoteLoader.USERNAME_PROMPT)
        password = C3RemoteLoader._getpass(C3RemoteLoader.PASSWORD_PROMPT)
        authz = '{}:{}'.format(user, password)
        base64authz = base64.b64encode(authz.encode()).decode() if IS_PY3 else base64.b64encode(authz)
        auth = 'Basic {}'.format(base64authz)

        url = urljoin(self._c3_server_url, 'auth/1/login')

        request = Request(url)
        request.add_header('Authorization', auth)

        try:
            response = urlopen(request)
        except HTTPError as e:
            if e.code == 401:
                raise ValueError('Wrong password for username "{}"'.format(user))
            raise
        except URLError as e:
            raise ValueError('Could not connect to "{}", please check URL and network'.format(url))
        if IS_PY3:
            cookie = response.getheader(C3RemoteLoader.COOKIE_HEADER_KEY)
            # Replace needed because of some difference between Python2 and Python3 SimpleCookie parsing
            cookie = cookie.replace(',', ';')
        else:
            cookie = response.info().getheader(C3RemoteLoader.COOKIE_HEADER_KEY)
        morsel = SimpleCookie(cookie).get('c3auth')   # TODO consider switching to oauth
        if morsel is None and response.code == 200:
            raise ValueError('Access denied. Request for "{}" to have access to "{}"'.format(user, self._c3_server_url))
        return morsel.value

    def _process_all(self):
        # Calculating checksum
        zip_checksum = hashlib.md5(self._zip_file).hexdigest()
        metadata_checksum = hashlib.md5(self._metadata_file).hexdigest() if self._metadata_file is not None else "_"

        # Create packages prefix to distinguish different packages, based on zip checksum
        self.package_prefix = 'Z{zip}M{metadata}'.format(zip=zip_checksum, metadata=metadata_checksum)

        # If this finder is already present in python system, no actions required here
        for finder in sys.meta_path:
            if isinstance(finder, self.__class__) and finder.package_prefix == self.package_prefix:
                break
        else:
            # No loaders match, do add new one
            self._process_code()

        if self._thin_and_define_types:
            self._process_c3_metadata()

    def _process_code(self):
        # Emulating real file
        zip_file_bytes = io.BytesIO(self._zip_file)

        # Extracted zip
        self._extracted_zip_file = zipfile.ZipFile(zip_file_bytes)

        # Creating set of files that exists in the package
        # to determine whether we should use this loader for loading packages
        self._paths = frozenset(os.path.join(self.package_prefix, x.filename) for x in self._extracted_zip_file.filelist)

        # Register this module as finder in python system
        sys.meta_path.append(self)

    def _process_c3_metadata(self):
        try:
            self._metadata = json.loads(self._metadata_file)
        except _JSONDecodeError as e:
            raise RuntimeError("Received C3 Types Metadata not well formatted: {}, {}"
                               .format(str(e), self._metadata_file))

    def _download_c3_py_zip(self):
        self._zip_file = self._request('/api/1/{tenant}/{tag}/remote/{mode}/python/c3.py.zip'.format(
            tenant=self._tenant,
            tag=self._tag,
            mode=self._mode
        ), 'application/zip')

    def _download_c3_metadata(self):
        self._metadata_file = self._request('/typesys/1/{tenant}/{tag}/all.json?minify'.format(
            tenant=self._tenant,
            tag=self._tag
        ))

    def _request(self, path, accept='application/json'):
        """ Downloading c3.py.zip from C3 Server """

        url = self._c3_server_url + path

        retry_num = 0

        while True:
            request = Request(url)
            request.add_header('Authorization', self._auth)
            request.add_header('Accept', accept)
            if path.startswith('/typesys/'):
                request.add_header('X-C3-Type-Aware', 'errorTemplate|timeseriesPrivateFields|Python')
            if self._action_id is not None:
                # Action id is required for python actions. It's used along with tokens in authorization process
                request.add_header('X-C3-Parent-Action-Id', self._action_id)
            try:
                response = urlopen(request)
                break
            except HTTPError as e:
                if e.getcode() == 401 and retry_num < 3:
                    retry_num += 1
                    if self._auth_fn:
                        self._auth = self._auth_fn()
                    # Refresh the url as well. This is primarily for the case where c3server initially provides the
                    # master url corresponding to the action auth token. The master url should be updated to the
                    # canonical load-balanced url.
                    if self._url_fn:
                        self._c3_server_url = self._parsed_url(self._url_fn())
                    if not self._auth_fn or retry_num > 1:
                        time.sleep(2.0)
                else:
                    response = e.read().decode('utf-8') if IS_PY3 else e.read()
                    raise RuntimeError('Could not download "{}-client" api from {}, {} {}'.format(self._mode, url, e, response))
            except URLError as e:
                raise RuntimeError('Could not download "{}-client" api from {}, {}'.format(self._mode, url, e))

        return response.read()

    def _mod_to_paths(self, fullname):
        """ Return fully-qualified package name if file belongs to this package """

        # Special case for prefix-package
        if fullname == self.package_prefix:
            return os.path.join(fullname, '__init__.py')

        # Special case for vendored packages
        if fullname.startswith(self.package_prefix + "._vendor"):
            package_init = os.path.join(fullname.replace(".", os.path.sep), "__init__.py")
            if package_init in self._paths:
                return package_init

        filename = fullname.replace(".", os.path.sep) + ".py"
        return filename if filename in self._paths else None

    def _get_source(self, filename):
        """ Return content of source file from zip by filename """

        # Removing package prefix
        filename = filename[len(self.package_prefix) + 1:]

        # Reading file form zip
        with self._extracted_zip_file.open(filename, 'r') as source:
            return source.read()

    # noinspection PyUnusedLocal
    def find_module(self, fullname, path=None):
        """
        This is method belongs to finder (https://docs.python.org/2/glossary.html#term-finder).
        It should return a loader (https://docs.python.org/2/glossary.html#term-loader) if it can load the given
        filename, and None if not.
        """
        return self if self._mod_to_paths(fullname) is not None else None

    def load_module(self, fullname):
        """
        Loading module by filename.
        See: https://www.python.org/dev/peps/pep-0302/
        """
        filename = self._mod_to_paths(fullname)

        module = sys.modules.setdefault(fullname, ModuleType(fullname))
        module.__file__ = filename
        module.__loader__ = self

        # Filename ends with __init__.py - it prefix-package, no source but need to add metadata
        if filename.endswith('__init__.py'):
            module.__path__ = []
            module.__package__ = fullname

            # Vendored packages expect to have their `__init__.py` file executed (since that is how normal Python
            # imports work)
            if "_vendor" in filename:
                source = self._get_source(filename)
                code = compile(source, fullname, 'exec')

                # Execute code in the module context
                exec(code, module.__dict__)
        else:
            module.__package__ = fullname.rpartition('.')[0]
            source = self._get_source(filename)

            # Compile source to show file names in stack traces
            code = compile(source, fullname, 'exec')

            # Execute code in the module context
            exec(code, module.__dict__)

        return module

    # noinspection PyMethodParameters
    def _masquerade_server_connection(this, module):
        """
        Change defaults arguments (url, auth, tenant and tag) for ServerConnection, without modifying functionality
        """

        class ServerConnection(module.ServerConnection):
            def __init__(self, url=None, auth=None, tenant=None, tag=None, action_id=None):
                super(ServerConnection, self).__init__(
                    url=url if url is not None else this._c3_server_url,
                    auth=auth if auth is not None else this._auth,
                    tenant=tenant if tenant is not None else this._tenant,
                    tag=tag if tag is not None else this._tag,
                    action_id=action_id if action_id is not None else this._action_id
                )
                self._auth_fn = this._auth_fn
                self._url_fn = this._url_fn

        return ServerConnection

    def _download_all(self):
        def download_and_catch(func):
            def run():
                try:
                    func()
                except BaseException as e:
                    self._download_error = e
            return run

        c3_py_zip_downloader = Thread(target=download_and_catch(self._download_c3_py_zip), name='c3.py.zip')
        c3_py_zip_downloader.start()

        if self._thin_and_define_types:
            metadata_downloader = Thread(target=download_and_catch(self._download_c3_metadata), name='metadata')
            metadata_downloader.start()
            metadata_downloader.join()

        c3_py_zip_downloader.join()

        if self._download_error:
            raise self._download_error

    def _load(self):
        """Loading C3 module to prefix and construction connection to C3 Server"""

        # Download data from C3 Server
        self._download_all()

        # Extract zip and add python module's finder and loader if required
        self._process_all()

        # Add package prefix to distinguish remotes from different C3 Servers
        package_name = self.package_prefix + ".C3"

        # Import package
        types = __import__(package_name, fromlist=[package_name])
        # Replace default arguments in ServerConnection
        types.ServerConnection = self._masquerade_server_connection(types)

        # Set metadata to module
        types._metadata = self._metadata

        for type_name in dir(types):
            if type_name.startswith('_'):
                continue
            if hasattr(self, type_name):
                raise Exception('Name conflict, invalid type name {}'.format(type_name))
            setattr(self, type_name, getattr(types, type_name))

    def get_typesys(self):
        '''
        Get type system based on configurations passed in during __init__
        '''
        connection = self.ServerConnection()
        if self._mode == C3RemoteLoader.Mode.THIN:
            typesys = connection.thinTypeSys()
            if self._thin_and_define_types:
                typesys._define_types(self._metadata)
            return typesys
        if self._mode == C3RemoteLoader.Mode.THICK:
            return connection.typeSys(define_types=self._define_types)

    @staticmethod
    def typeSys(url=None, tenant=None, tag=None, auth=None, define_types=False, action_id=None,
                mode=Mode.THICK):
        """
        Get the C3 type system. This is the recommended method to gain access to C3 Remote.

        c3 = C3RemoteLoader.typeSys(url='https://mycompany.c3iot.com/')
        print(c3.User.fetch().objs)

        :param url: The URL you would like to connect to, eg 'https://mycompany.c3iot.com/'
        :param tenant: Tenant to connect to
        :param tag: Tag to connect to
        :param auth: The auth token
        :param define_types: True iff you want to preload all type metadata and define types and inline functions
        :param action_id: The id of the action, usually only used when calling on server
        :return: The c3 type system corresponding to a given tenant and tag
        """
        if not IS_PY3 and mode == C3RemoteLoader.Mode.THICK:
            raise RuntimeError("Thick python remote is not supported for python 2. "
                               "Please change runtime to python 3 or set mode to 'thin'.")

        # Setup the c3 type system extended help().
        global __builtin_help__
        global __type_sys__
        # Overriding the help() builtin, if not already done.
        # This could be done by reassigning builtins.help, however (as it is in this case)
        # if invoked via exec(), this should be done through the globals() dictionary:
        # See https://docs.python.org/3/library/functions.html#exec
        if __builtin_help__ is None:
            __builtin_help__ = globals()['__builtins__']['help']
            globals()['__builtins__']['help'] = _c3_help if mode == C3RemoteLoader.Mode.THICK else _c3_help_thin

        loader = C3RemoteLoader(url=url, tenant=tenant, tag=tag, auth=auth, mode=mode,
                                action_id=action_id, define_types=define_types)
        __type_sys__ = loader.get_typesys()
        return __type_sys__

    @staticmethod
    def connect(url=None, tenant=None, tag=None, auth=None, mode=Mode.REST):
        """
        You should usually use C3RemoteLoader.thinTypeSys, only use this if you have a special case (eg would like to hit REST APIs).

        C3RemoteLoader = C3RemoteLoader.make(url='https://mycompany.c3iot.com/', tenant='machineLearning', tag='prod', mode=C3RemoteLoader.Mode.REST)
        c3conn = C3RemoteLoader.ServerConnection()
        print(c3conn.request(path='/health/1/?info').send().statusCode) # 200
        """
        return C3RemoteLoader(url, tenant, tag, auth, mode).ServerConnection()
