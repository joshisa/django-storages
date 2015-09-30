from django.core.files.base import ContentFile
from django.core.exceptions import ImproperlyConfigured
from storages.compat import deconstructible, Storage

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
        contents = self.connection[self.container_name][name].read()
        return ContentFile(contents)

    def exists(self, name):
        self.connection[name].exists()

    def delete(self, name):
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
        print("Saving content data ...")
        saveresult = self.connection[self.container_name][name].send(content_data.decode("ISO-8859-1"))
        return name

    def url(self, name):
        return "{}/{}/{}".format(self.connection.properties['url'], self.container_name, name)
