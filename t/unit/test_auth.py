import ssl
from faust.auth import GSSAPICredentials, SASLCredentials, SSLCredentials
from faust.types.auth import AuthProtocol, SASLMechanism
from mode.utils.mocks import patch


class test_SASLCredentials:

    def test_constructor(self):
        c = SASLCredentials(username='george', password='pw1')
        assert c.username == 'george'
        assert c.password == 'pw1'
        assert repr(c)
        assert c.mechanism == SASLMechanism.PLAIN
        assert c.protocol == AuthProtocol.SASL_PLAINTEXT

        c2 = SASLCredentials(
            username='george', password='pw1', mechanism='GSSAPI')
        assert c2.mechanism == SASLMechanism.GSSAPI
        c3 = SASLCredentials(
            username='george', password='pw1', mechanism=SASLMechanism.GSSAPI)
        assert c3.mechanism == SASLMechanism.GSSAPI


class test_GSSAPICredentials:

    def test_constructor(self):
        c = GSSAPICredentials(
            kerberos_service_name='george',
            kerberos_domain_name='domain',
        )
        assert c.kerberos_service_name == 'george'
        assert c.kerberos_domain_name == 'domain'
        assert repr(c)
        assert c.mechanism == SASLMechanism.GSSAPI
        assert c.protocol == AuthProtocol.SASL_PLAINTEXT

        c2 = GSSAPICredentials(
            kerberos_service_name='george',
            kerberos_domain_name='domain',
            mechanism='PLAIN',
        )
        assert c2.mechanism == SASLMechanism.PLAIN
        c3 = GSSAPICredentials(
            kerberos_service_name='george',
            kerberos_domain_name='domain',
            mechanism=SASLMechanism.PLAIN,
        )
        assert c3.mechanism == SASLMechanism.PLAIN


class test_SSLCredentials:

    def test_constructor(self):
        with patch('faust.auth.ssl.create_default_context') as cdc:
            c = SSLCredentials(
                purpose=ssl.Purpose.SERVER_AUTH,
                cafile='/foo/bar/ca.file',
                capath='/foo/bar/',
                cadata='moo',
            )
            assert c.context is cdc.return_value
            assert repr(c)
            cdc.assert_called_once_with(
                purpose=ssl.Purpose.SERVER_AUTH,
                cafile='/foo/bar/ca.file',
                capath='/foo/bar/',
                cadata='moo',
            )
