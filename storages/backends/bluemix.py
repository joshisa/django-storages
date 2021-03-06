from django.core.files.base import ContentFile
from django.core.exceptions import ImproperlyConfigured
from storages.compat import deconstructible, Storage
from urllib.parse import urlparse
import urllib.request
import mimetypes

import os
import mimetypes

try:
    import object_storage
    from object_storage.errors import NotFound
except ImportError:
    raise ImproperlyConfigured(
        "Could not load Softlayer bindings. "
        "See https://github.com/softlayer/softlayer-object-storage-python")

from storages.utils import setting


def clean_name(name):
    return os.path.normpath(name).replace("\\", "/")

@deconstructible
class BluemixStorage(Storage):
    username = setting("BLUEMIX_OBJSTOR_USERNAME")
    api_key = setting("BLUEMIX_OBJSTOR_PASSWORD")
    auth_url = setting("BLUEMIX_OBJSTOR_AUTH_URL")
    container_name = setting("BLUEMIX_OBJSTOR_CONTAINER")
    cluster = setting("BLUEMIX_OBJSTOR_CLUSTER")

    def __init__(self, *args, **kwargs):
        super(BluemixStorage, self).__init__(*args, **kwargs)
        self._connection = None

    @property
    def connection(self):
        if self._connection is None:
            self._connection = object_storage.get_client(self.username,
                                                         self.api_key,
                                                         self.auth_url,
                                                         datacenter=self.cluster)
        return self._connection

    def _open(self, name, mode="rb"):
        if name.startswith('http'):
            print("Opening a URL Resource @ %s" % name)
            with urllib.request.urlopen(name) as response:
                content_type = response.headers.get_content_charset() or mimetypes.guess_type(name)[0]
                contents = response.read()
        else:
            contents = self.connection[self.container_name][name].read().encode('utf-8')
        return ContentFile(contents)

    def exists(self, name):
        if name.startswith('http'):
            deconstruct = urlparse(name)
            # https://dal05.objectstorage.softlayer.net/v1/AUTH_blah/foo/user/0/1/2/3/4/imagename.jpg
            # array = ["https://dal05.objectstorage.softlayer.net", "v1", "AUTH_blah", "foo", "user/0/1/2/3/4/imagename.jpg"]
            # array[4] == path.
            name = '/%s' % deconstruct.path.split('/', maxsplit=4)[4]
            print("EXISTS: Resolved partial name is %s" % name)
        return self.connection[self.container_name][name].exists()

    def delete(self, name):
        if name.startswith('http'):
            deconstruct = urlparse(name)
            name = '/%s' % deconstruct.path.split('/', maxsplit=4)[4]
            print("DELETE: Resolved partial name is %s" % name)
        if self.connection[self.container_name][name].exists():
            self.connection[self.container_name][name].delete()

    def size(self, name):
        properties = self.connection[self.container_name][name].properties
        return properties["size"]

    def _save(self, name, content):
        if hasattr(content.file, 'content_type'):
            content_type = content.file.content_type
        else:
            content_type = mimetypes.guess_type(name)[0]

        if hasattr(content, 'chunks'):
            content_data = b''.join(chunk for chunk in content.chunks())
        else:
            content_data = content.read()
        if not self.connection[self.container_name].exists():
            print("Container not found.  Creating ...")
            self.connection[self.container_name].create()
            # Need to make the container public
            publicresult = self.connection[self.container_name].make_public()
        else:
            # Need to ensure the container is public
            publicresult = self.connection[self.container_name].make_public()
        if not self.connection[self.container_name][name].exists():
            self.connection[self.container_name][name].create()
        print("Saving content data to Bluemix v1 Object Storage...")
        saveresult = self.connection[self.container_name][name].write(content_data)
        return name
        
    def get_available_name(self, name):
        """
        Directly Returns a filename that's 
        from what user input.
        """
        # if self.exists(name):
        # Remove the existing file
        #    self.delete(name)
        # Return the input name as output
        return name

    def url(self, name):
        return "{}/{}/{}".format(self.connection.properties['url'],self.container_name, name)
    
    def path(self, name):
        return "{}/{}/{}".format(self.connection.properties['url'],self.container_name, name)
