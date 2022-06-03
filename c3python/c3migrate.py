import os
from c3python import get_c3
from c3python import AzureBlob
        
def retry_c3(c3m,retries=3):
    left = {'retries': retries}

    def decorator(f):
        def inner(*args, **kwargs):
            while left['retries']:
                try:
                    return f(*args, **kwargs)
                except RuntimeError as e:
                    print (e)
                    left['retries'] -= 1
                    print(f"Retrying...")
                    c3m.set_c3_objects()
                    print(f"Retries Left: {left['retries']}.")
            raise Exception("Retried {} times".format(retries))
        return inner
    return decorator



class C3Migrate:
    
    def __init__(
        self, from_url, to_url,
        from_tenant, to_tenant,
        from_tag, to_tag, 
        from_keyfile=None,to_keyfile=None,
        from_auth=None, to_auth=None,
        toBlobConnectionStringFile=None,
        toBlobContainer=None,
        retrys=3
        ):
        self.from_url = from_url
        self.to_url = to_url
        self.from_tenant = from_tenant
        self.to_tenant = to_tenant
        self.from_tag = from_tag
        self.to_tag = to_tag
        self.from_keyfile = from_keyfile
        self.to_keyfile = to_keyfile
        self.from_auth = from_auth
        self.to_auth = to_auth
        self.retrys = retrys
        self.tries = 0
        self.tryFetch = retry_c3(self,retrys)(self.tryFetch)
        self.tryRemoveAll = retry_c3(self,retrys)(self.tryRemoveAll)
        self.tryMergeBatch = retry_c3(self,retrys)(self.tryMergeBatch)
        self.tryUpdateFileMetadata = retry_c3(self,retrys)(self.tryUpdateFileMetadata)

        self.set_c3_objects()

        if toBlobConnectionStringFile and toBlobContainer:
            with open(toBlobConnectionStringFile, 'r') as f:
                toConnectionString = f.readline().strip()
            self.copyObj = AzureBlob.CopyUrlsToContainer(toBlobContainer, toConnectionString)

    @staticmethod
    def tryFetch(c3,typeName,spec):
        return getattr(c3,typeName).fetch(spec)

    @staticmethod
    def tryRemoveAll(c3,typeName):
        print(f'Removing {typeName} data from {c3.connection.url()}...', end='')
        result = getattr(c3,typeName).removeAll()
        print(" Done.")
        return result

    @staticmethod
    def tryMergeBatch(c3,typeName,objs):
        print(f"Merging {len(objs)} to {typeName}...", end='')
        result = getattr(c3,typeName).mergeBatch(objs.toJson())
        print(" Done.")
        return result

    @staticmethod
    def tryUpdateFileMetadata(c3,objFiles,fileField):
        updates = []
        fileType = list(objFiles.values())[0]['obj'].toJson()['type']
        print(f"Reading metadata for {len(objFiles)} {fileType} files...", end="")
        for oDict in objFiles.values():
            #print(oDict['url'])
            obj = oDict['obj']
            url = oDict['url']
            
            updated = getattr(c3,fileType)(**{'id':obj.id})
            setattr(updated,fileField,c3.File(**{'url': url}).readMetadata())
            if not getattr(updated,fileField).contentLength:
                #print(url)
                setattr(updated,fileField, getattr(updated,fileField).fs().listFiles(url).files[0].toJson())
            updates.append(updated)
        print(f" Done.")
        print(f"Merging updates for {len(updates)} files.")
        _ = getattr(c3,fileType).mergeBatch(updates)
        return {
                    o.id:{'obj':o,'url':getattr(o, fileField).url}
                    for o in updates if getattr(o, fileField)
                }

    def set_c3_objects(self):
        self.c3_from = get_c3(url=self.from_url, tenant=self.from_tenant, tag=self.from_tag, keyfile=self.from_keyfile, auth=self.from_auth)
        self.c3_to = get_c3(url=self.to_url, tenant=self.to_tenant, tag=self.to_tag, keyfile=self.to_keyfile, auth=self.to_auth)
        self.fromFS = self.c3_from.FileSystem.inst()
        self.toFS = self.c3_to.FileSystem.inst()
    
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
        """ Depricated. Use copyFilesFromType instead. """
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
        
    def migrate_type(self, type, batch_size=1000, remove_to=False, reset_version=False):
        has_more = True
        offset = 0
        print(f'Migrating {type} from {self.c3_from.connection.url()} to {self.c3_to.connection.url()}')
        if remove_to:
            self.tryRemoveAll(self.c3_to,type)
        while(has_more):
            result = self.tryFetch(self.c3_from, type, {"limit":batch_size,"offset":offset})
            objs = result.objs
            has_more = result.hasMore
            offset += batch_size
            if reset_version:
                for obj in objs:
                    #print(f"Hey version: {obj.version}")
                    obj.version = None
            self.tryMergeBatch(self.c3_to, type, objs)
                    
    def migrate_files(self, type, file_field, batch_size=1000,local_path='/tmp',filter=None):
        """ Depricated. Use copyFilesFromType instead. """
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
            
    def migrate_types(self, types, remove_to=False,batch_size=1000):
        for type in types:
            self.migrate_type(type, remove_to=remove_to,batch_size=batch_size)

    # def generateSignedUrlList(self, objs, file_fields):

    #     FS=self.c3_from.FileSystem.inst()

    #     return [
    #         FS.generatePresignedUrl(getattr(o, field).url)
    #         for o in objs
    #         for field in file_fields
    #     ]
    
    def copyFilesFromType(self, typeName, file_fields, batch_size=500,dry_run=False,single_batch=False):
        """ Copy files from one C3 instance to another.
        Replacement for migrate_files
        """
        prefix='azure://'
        has_more = True
        offset = 0
        file_count = getattr(self.c3_from, typeName).fetchCount()
        print(f'Copying files from {file_count} rows in {typeName}.{file_fields} from {self.c3_from.connection.url()} to {self.c3_to.connection.url()}')
        toMountUrl = self.toFS.mountUrl()
        toContainer=toMountUrl.split(prefix)[1].split('/')[0]
        fromMountUrl = self.fromFS.mountUrl()
        fromContainer=fromMountUrl.split(prefix)[1].split('/')[0]
        batch = 1
        
        while(has_more):
            print (f"Copying batch {batch} of {-(file_count // -batch_size)}")
            result = self.tryFetch(self.c3_from, typeName, {"limit":batch_size,"offset":offset})
            objs = result.objs
            has_more = result.hasMore
            
            # Check if file type is nested in another type
            for f in file_fields:
                if len(f.split('.')) > 1:
                    outer,inner = f.split('.')
                    print(f"nested:{outer}:{inner}")
            # Warn for rows that don't have files
            for o in objs:
                #print(o)
                for f in file_fields:
                    if len(f.split('.')) > 1:
                        outer,inner = f.split('.')
                        oo = getattr(o,outer)
                        if not getattr(oo, inner):
                            print (f"WARNING: {typeName}.{f}.{outer} is empty for {o.id}")
                    else:
                        if not getattr(o, f):
                            print (f"WARNING: {typeName}.{f} is empty for {o.id}")

            #end test nested mods so far...
            
            # Generate signed urls)
            print(f"Generating signed urls for {len(objs)*len(file_fields)} files...",end="")
            fromSignedFiles = [
                self.fromFS.generatePresignedUrl(getattr(o, field).url)
                for o in objs
                for field in file_fields if getattr(o, field)
            ]
            print(" Done.")

            # Generate list destination Urls
            toFiles = [
                getattr(o, field).url.replace(fromMountUrl,toMountUrl).split(prefix+toContainer+'/')[1]
                for o in objs
                for field in file_fields if getattr(o, field)
            ]

            # Copy files
            if dry_run:
                # print(len(fromSignedFiles))
                print(len(toFiles))
            else:
                print(f"Copying {len(fromSignedFiles)} files...")
                self.copyObj.copyUrls(fromSignedFiles, toFiles)
                print("Done copying files.")

            # Update c3 objects with new urls and metadata
            print("Update Metadata")
            for field in file_fields:
                #print(f"Migrating {typeName}.{field}")
                objFiles = {
                    o.id:{'obj':o,'url':getattr(o, field).url.split(fromMountUrl)[1]}
                    for o in objs if getattr(o, field)
                }
                if dry_run:
                    print(objFiles)

                updatedObjFiles = self.tryUpdateFileMetadata(self.c3_to, objFiles, field)
                # try:
                #     updatedObjFiles = updateFileMetadata(self.c3_to,objFiles,field)
                #     if dry_run:
                #         print("Updated Obj Files:")
                #         print(updatedObjFiles)
                # except RuntimeError:
                #     if self.tries < self.retrys:
                #         self.tries += 1
                #         print(f'Retrying {self.tries}')
                #         self.set_c3_objects()
                #         updatedObjFiles = updateFileMetadata(self.c3_to,objFiles,field)
                # # for o in updatedObjs:
                # #     print(f"Updated {o.id}")
                print(f"Comparing source and dest file size and MD5")
                for id,oDict in updatedObjFiles.items():
                    #print(oDict['url'])
                    toObj = oDict['obj']
                    fromObj = objFiles[id]['obj']
                    toLen = getattr(toObj, field).contentLength
                    fromLen = getattr(fromObj, field).contentLength
                    toMD5 = getattr(toObj, field).contentMD5
                    fromMD5 = getattr(fromObj, field).contentMD5
                    assert toLen == fromLen, f"{toLen} != {fromLen} for {toObj.id}"
                    assert toMD5 == fromMD5, f"{toMD5} != {fromMD5} for {toObj.id}"           
            if single_batch:
                break
            offset += batch_size
            batch+=1 