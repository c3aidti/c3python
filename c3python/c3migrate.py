import os
from c3python import get_c3

class C3Migrate:
    
    def __init__(self, from_url, from_tenant, from_tag, from_keyfile, to_url, to_tenant, to_tag, to_keyfile,retrys=3):
        self.from_url = from_url
        self.from_tenant = from_tenant
        self.from_tag = from_tag
        self.from_keyfile = from_keyfile
        self.to_url = to_url
        self.to_tenant = to_tenant
        self.to_tag = to_tag
        self.to_keyfile = to_keyfile
        self.retrys = retrys
        self.tries = 0
        self.set_c3_objects()
        
    def set_c3_objects(self):
        self.c3_from = get_c3(url=self.from_url, tenant=self.from_tenant, tag=self.from_tag, keyfile=self.from_keyfile)
        self.c3_to = get_c3(url=self.to_url, tenant=self.to_tenant, tag=self.to_tag, keyfile=self.to_keyfile)
    
    def check_type(self, type):
        print(f'checking {type}')
        from_count = getattr(self.c3_from, type).fetchCount()
        to_count = getattr(self.c3_to, type).fetchCount()
        return {'from_count':from_count, 'to_count':to_count}
    
    def check_types(self, types):
        for type in types:
            counts = self.check_type(type)
            print(f'{type} from: {counts["from_count"]} to: {counts["to_count"]}')
            
    def migrate_file(self, obj, file_field, local_path='/tmp',count = 0, cntr=0):
        url = getattr(obj, file_field).url
        filename = os.path.basename(url)
        tmp_path = local_path + '/' + filename
        print(f"Migrating {cntr} of {count}: {filename}")
        # if not os.path.exists(tmp_path):
        #     os.system(f"curl -o {tmp_path} {url}")
        #setattr(obj, file_field, tmp_path)
        #self.c3_to.save(obj)
        self.c3_from.Client.copyFilesToLocalClient(url,local_path)
        self.c3_to.Client.uploadLocalClientFiles(tmp_path, url, {"peekForMetadata": True})
        os.remove(tmp_path)
        
    def migrate_type(self, type, batch_size=1000, remove_to=False):
        has_more = True
        offset = 0
        print(f'Migrating {type} from {self.c3_from.connection.url()} to {self.c3_to.connection.url()}')
        if remove_to:
            print(f'Removing {type} data from {self.c3_to.connection.url()}')
            getattr(self.c3_to, type).removeAll()
        while(has_more):
            result = getattr(self.c3_from, type).fetch(spec={"limit":batch_size,"offset":offset})
            objs = result.objs
            has_more = result.hasMore
            offset += batch_size
            
            getattr(self.c3_to, type).upsertBatch(objs.toJson())
                    
    def migrate_files(self, type, file_field, batch_size=1000,local_path='/tmp',filter=None):
        has_more = True
        offset = 0
        file_count = getattr(self.c3_from, type).fetchCount()
        print(f'Migrating {file_count} files in {type}.{file_field} from {self.c3_from.connection.url()} to {self.c3_to.connection.url()}')
        while(has_more):
            result = getattr(self.c3_from, type).fetch(spec={"limit":batch_size,"offset":offset})
            objs = result.objs
            has_more = result.hasMore
            
            for cnt,o in enumerate(objs):
                try:
                    self.migrate_file(o,file_field,local_path=local_path, count=file_count, cntr=offset+cnt+1)
                except RuntimeError:
                    if self.tries < self.retrys:
                        self.tries += 1
                        print(f'Retrying {self.tries}')
                        self.set_c3_objects()
                        self.migrate_file(o,file_field,local_path=local_path, count=file_count, cntr=offset+cnt+1)
            offset += batch_size
            
    def migrate_types(self, types, remove_to=False):
        for type in types:
            self.migrate_type(type, remove_to=remove_to)