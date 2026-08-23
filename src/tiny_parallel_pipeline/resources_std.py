"""Common resource types: a file on disk, a piece of text, a URL."""


from typing import override


from tiny_parallel_pipeline import ResourceStatus, Resource


class FileResource(Resource):
    """A file the pipeline will create. `data` becomes its path once it is there."""
    def __init__(self, in_class_id: str, expect_ready_file_at: str, is_ready: bool = False):
        super().__init__(in_class_id=in_class_id)
        self.expect_ready_file_at = expect_ready_file_at
        if is_ready:
            self.populate_data(expect_ready_file_at).update_status(ResourceStatus.READY)

    @override
    def populate_data(self, new_data: any):
        assert new_data == self.expect_ready_file_at, f'{new_data} != {self.expect_ready_file_at}'
        return super().populate_data(new_data)


class TxtResource(Resource):
    def __init__(self, in_class_id: str, ready_txt_data: str | None = None):
        super().__init__(in_class_id=in_class_id)
        if ready_txt_data is not None:
            self.populate_data(ready_txt_data).update_status(ResourceStatus.READY)


class UrlStrResource(Resource):
    def __init__(self, in_class_id: str, url_address_data: str | None = None):
        super().__init__(in_class_id=in_class_id)
        if url_address_data is not None:
            self.populate_data(url_address_data).update_status(ResourceStatus.READY)
