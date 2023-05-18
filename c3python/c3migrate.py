import os
from turtle import up
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
        from_user=None, to_user=None,
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
        self.from_user = from_user
        self.to_user = to_user
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
    def tryRemoveAll(c3,typeName,filter=None):
        print(f'- Removing {typeName} data from {c3.connection.url()}...', end='')
        if filter:
            result = getattr(c3,typeName).removeAll(removeFilter=filter)
        else:
            result=getattr(c3,typeName).removeAll()
        print(" Done.")
        return result

    @staticmethod
    def tryMergeBatch(c3,typeName,objs):
        print(f"Merging {len(objs)} to {typeName}...", end='')
        result = getattr(c3,typeName).mergeBatch(objs.toJson())
        print(" Done.")
        return result

    @staticmethod
    def tryUpdateFileMetadata(c3,objFiles,fileField,remove_file_keys=None,url_only=False):
        updates = []
        if len(objFiles) <= 0:
            print("- No files to update.")
            return
        fileType = list(objFiles.values())[0]['obj'].toJson()['type']
        print(f"- Reading metadata for {len(objFiles)} {fileType} files...", end="")
        HAVE_SET_URL_ONLY = False
        for oDict in objFiles.values():
            #print("oDict_url", oDict['url'])
            obj = oDict['obj']
            url = oDict['url']
            
            updated = getattr(c3,fileType)(**{'id':obj.id})
            #setattr(updated,fileField, getattr(updated,fileField).fs().listFiles(url).files[0].toJson())
            
            if url_only:
                # print ("\n  - url_only")
                if len(fileField.split('.')) > 1:
                    outer,inner = f.split('.')
                    oo = getattr(updated,outer)
                    setattr(oo,inner, c3.File(**{'url': url}))
                else:
                    setattr(updated,fileField,c3.File(**{'url': url}))
                    HAVE_SET_URL_ONLY = True
            else:
                if len(fileField.split('.')) > 1:
                    outer,inner = fileField.split('.')
                    oo = getattr(updated,outer)
                    if oo:
                        setattr(oo,inner,c3.File(**{'url': url}).readMetadata().toJson())
                        if not getattr(oo,inner).contentLength:
                            setattr(oo,inner, getattr(oo,inner).fs().listFiles(url).files[0].toJson())
                            if not getattr(oo,inner).contentLength:
                                setattr(oo,inner,c3.File(**{'url': url}))
                                HAVE_SET_URL_ONLY = True

                else:
                    # setattr(updated,fileField, getattr(updated,fileField).fs().listFiles(url).files[0].toJson())
                    setattr(updated,fileField,c3.File(**{'url': url}).readMetadata().toJson())
                    # print(f"REAL: {c3.File(**{'url': url}).readMetadata().toJson()}")
                    # print(f"TESTTTTT: {updated.toJson()}")
                    if not getattr(updated,fileField).contentLength:
                        # print("\n  - contentLength not found")
                        # print(url)
                        # print(getattr(updated,fileField).fs().listFiles(url).files[0].toJson())
                        setattr(updated,fileField, getattr(updated,fileField).fs().listFiles(url).files[0].toJson())
                        if not getattr(updated,fileField).contentLength:
                            #print("\n  - contentLength not found again")
                            HAVE_SET_URL_ONLY = True
                            setattr(updated,fileField,c3.File(**{'url': url}))
             # Remove file_keys
            if remove_file_keys:
                #print("\n  - remove_file_keys")
                for key in remove_file_keys:
                    if key in getattr(updated,fileField):
                        delattr(updated,fileField)[key]
            
            #print("\n  - updated", updated.toJson())
            updates.append(updated)
        if HAVE_SET_URL_ONLY:
            print("- Done. (Some files were set to url only, you may want to run updateMetadata again.)")
        print(f"- Done.")
        print(f"Merging updates for {len(updates)} files.")
        _ = getattr(c3,fileType).mergeBatch(updates)
        return_dict = {}
        if len(fileField.split('.')) > 1:
            outer,inner = fileField.split('.')
            #oo = getattr(c3,outer)
            for o in updates:
                oo = getattr(o,outer)
                if oo:
                    return_dict[o.id] = {'obj': o,'url':getattr(oo,inner).url}
        else:
            for o in updates:
                return_dict[o.id] = {'obj':o,'url':getattr(o, fileField).url}
        return return_dict
        # return {
        #             o.id:{'obj':o,'url':getattr(o, fileField).url}
        #             for o in updates if getattr(o, fileField)
        #         }

    def set_c3_objects(self):
        self.c3_from = get_c3(url=self.from_url, tenant=self.from_tenant, tag=self.from_tag, keyfile=self.from_keyfile, username=self.from_user, auth_token=self.from_auth)
        self.c3_to = get_c3(url=self.to_url, tenant=self.to_tenant, tag=self.to_tag, keyfile=self.to_keyfile, username=self.to_user, auth_token=self.to_auth)
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
        
    def migrate_type(self, type, batch_size=1000, remove_to=False, reset_version=False, reset_fields=None,filter=None):
        has_more = True
        offset = 0
        print(f'Migrating "{type}"(s) from {self.c3_from.connection.url()} to {self.c3_to.connection.url()}')
        if remove_to:
            self.tryRemoveAll(self.c3_to,type,filter)
        while(has_more):
            spec = {"limit":batch_size,"offset":offset}
            if filter:
                spec['filter'] = filter
            result = self.tryFetch(self.c3_from, type, spec)
            objs = result.objs
            has_more = result.hasMore
            offset += batch_size
            if reset_version:
                for obj in objs:
                    #print(f"Hey version: {obj.version}")
                    obj.version = None
            if reset_fields:
                for obj in objs:
                    for field in reset_fields:
                        setattr(obj, field, None)
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
            
    def migrate_types(self, types, remove_to=False,batch_size=1000,reset_version=False):
        for type in types:
            self.migrate_type(type, remove_to=remove_to,batch_size=batch_size,reset_version=reset_version)

    # def generateSignedUrlList(self, objs, file_fields):

    #     FS=self.c3_from.FileSystem.inst()

    #     return [
    #         FS.generatePresignedUrl(getattr(o, field).url)
    #         for o in objs
    #         for field in file_fields
    #     ]

    def updateMetadataForFilesFromType(self, typeName, file_fields, batch_size=500,dry_run=False,single_batch=False, filter=None,remove_file_keys=None,url_only=False,reset_version=False,update_metadata=True):
        """ Copy files from one C3 instance to another.
        Replacement for migrate_files
        """
        prefix='azure://'
        has_more = True
        offset = 0
        file_count = getattr(self.c3_from, typeName).fetchCount()
        print("")
        print(f'Updating metadata for files from {file_count} rows in {typeName}.{file_fields} from {self.c3_from.connection.url()} to {self.c3_to.connection.url()}')
        toMountUrl = self.toFS.mountUrl()
        toContainer=toMountUrl.split(prefix)[1].split('/')[0]

        fromMountUrl = self.fromFS.mountUrl()
        # print(f"toContainer: {toContainer}")
        # print(f"toMountUrl: {toMountUrl}")

        fromContainer=fromMountUrl.split(prefix)[1].split('/')[0]
        # print(f"fromContainer: {fromContainer}")
        # print(f"fromMountUrl: {fromMountUrl}")

        batch = 1
        is_nested = [False for f in file_fields]
        has_more = True
        while(has_more):
            print (f"updating batch {batch} of {-(file_count // -batch_size)}")
            spec = {"limit":batch_size,"offset":offset}
            if filter:
                spec['filter'] = filter
            result = self.tryFetch(self.c3_to, typeName, spec)
            objs = result.objs
            has_more = result.hasMore
            updated = []
            for cntr,f in enumerate(file_fields):
                    if len(f.split('.')) > 1:
                        outer,inner = f.split('.')
                    else:
                        for o in objs:
                            # print(f"  - setting: {self.c3_to.File(**{'url':  getattr(o,f).url}).readMetadata().toJson()}")
                            if hasattr(getattr(o,f),'url'):
                                setattr(o, f,self.c3_to.File(**{'url': getattr(o,f).url}).readMetadata().toJson() )
                            else:
                                print(f"\nWARNING - {o.id} has no url for {f}")
                                print(f"WARNING - Failed to update metadata for {o.id} in {typeName}.{f}")
            getattr(self.c3_to,typeName).mergeBatch(objs)
            updatedObjFiles = {
                    o.id:{'obj':o,'url':getattr(o, f).url}
                    for f in file_fields
                    for o in objs if getattr(o, f)
                }
            
            fromResult = self.tryFetch(self.c3_from, typeName, spec)
            fromObjs = result.objs
            fromObjsDict = {o.id:o for o in fromObjs}
            for f in file_fields:
                for o in objs:
                    #print(oDict['url'])
                    toObj = o
                    fromObj = fromObjsDict[o.id]
                    if hasattr(getattr(toObj,f),'url'):
                        toLen = getattr(toObj, f).contentLength
                        fromLen = getattr(fromObj, f).contentLength
                        toMD5 = getattr(toObj, f).contentMD5
                        fromMD5 = getattr(fromObj, f).contentMD5
                        assert toLen == fromLen, f"{toLen} != {fromLen} for {toObj.id}"
                        assert toMD5 == fromMD5, f"{toMD5} != {fromMD5} for {toObj.id}"  
            if single_batch:
                break
            offset += batch_size
            batch+=1 

    def copyFilesFromType(self, typeName, file_fields, batch_size=500,dry_run=False,single_batch=False, filter=None,include=None,remove_file_keys=None,url_only=False,reset_version=False,update_metadata=True,offset=None):
        """ Copy files from one C3 instance to another.
        Replacement for migrate_files
        """
        prefix='azure://'
        has_more = True
        if not offset:
            offset = 0
        file_count = getattr(self.c3_from, typeName).fetchCount()
        if offset:
            file_count = file_count - offset
        print("")
        print(f'Copying files from {file_count} rows in {typeName}.{file_fields} from {self.c3_from.connection.url()} to {self.c3_to.connection.url()}')
        print(f"Using batch size: {batch_size}")
        toMountUrl = self.toFS.mountUrl()
        print(toMountUrl)
        toContainer=toMountUrl.split(prefix)[1].split('/')[0]

        fromMountUrl = self.fromFS.mountUrl()
        # print(f"toContainer: {toContainer}")
        # print(f"toMountUrl: {toMountUrl}")

        fromContainer=fromMountUrl.split(prefix)[1].split('/')[0]
        # print(f"fromContainer: {fromContainer}")
        # print(f"fromMountUrl: {fromMountUrl}")

        batch = 1
        is_nested = [False for f in file_fields]
        #print (is_nested)
        while(has_more):
            print (f"Copying batch {batch} of {-(file_count // -batch_size)}")
            spec = {"limit":batch_size,"offset":offset}
            if filter:
                spec['filter'] = filter
            if include:
                spec['include'] = include
            result = self.tryFetch(self.c3_from, typeName, spec)
            objs = result.objs
            has_more = result.hasMore

                    # if remove_file_keys: # Nullifies a field in the destination
                    #     for o in objs:
                    #         for field in remove_file_keys:
                    #             setattr(o, field, None)

            # if remove_file_keys:
            #     for k in remove_file_keys:
            #         if len(k.split('.')) > 1:
            #             for o in objs:
            #                 outer,inner = k.split('.')
            #                 oo = getattr(o,outer)
            #                 setattr(oo, inner, None)

            #         else:
            #             for o in objs:
            #                 setattr(o, k, None)
            if reset_version:
                for o in objs:
                    o.version = None
            
            # Check if file type is nested in another type
            for cntr,f in enumerate(file_fields):
                is_nested[cntr] = False
                if len(f.split('.')) > 1:
                    outer,inner = f.split('.')
                    # print(f"nested:{outer}:{inner}")
                    is_nested[cntr] = True

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
            fromSignedUrls = []
            toUrls = []
            for cntr,f in enumerate(file_fields):
                if len(f.split('.')) > 1:
                    outer,inner = f.split('.')
                    for o in objs:
                        if getattr(getattr(o,outer),inner):
                            toUrls.append(
                                getattr(getattr(o,outer),inner).url.replace(fromMountUrl,toMountUrl).split(prefix+toContainer+'/')[1]
                            )
                            fromSignedUrls.append(
                                self.fromFS.generatePresignedUrl(getattr(getattr(o,outer),inner).url)
                            )
                    # fromSignedUrls.append(
                    #     self.fromFS.generatePresignedUrl(getattr(getattr(o,outer),inner).url)
                    #     for o in objs
                    # )
                    # toUrls.append(
                    #     getattr(getattr(o,outer),inner).url.replace(fromMountUrl,toMountUrl).split(prefix+toContainer+'/')[1]
                    # )
                else:
                    for o in objs:
                        if getattr(o, f):
                            fromSignedUrls.append(self.fromFS.generatePresignedUrl(getattr(o, f).url))
                            toUrls.append(
                                getattr(o, f).url.replace(fromMountUrl,toMountUrl).split(prefix+toContainer+'/')[1]
                            )
                    # fromSignedUrls.append(
                    #     self.fromFS.generatePresignedUrl(getattr(o, f).url)
                    #     for o in objs
                    # )
            
            # old code
            # fromSignedUrls = [
            #     self.fromFS.generatePresignedUrl(getattr(o, field).url)
            #     for o in objs
            #     for field in file_fields if getattr(o, field,None)
            # ]
            print(" Done.")

            # # Generate list destination Urls
            # toUrls = []
            # for cntr,f in enumerate(file_fields):
            #     if len(f.split('.')) > 1:
            #         outer,inner = f.split('.')
            #         toUrls.append(
            #             #self.toFS.url(getattr(getattr(o,outer),inner).path)
            #             getattr(getattr(o,outer),inner).url.replace(fromMountUrl,toMountUrl).split(prefix+toContainer+'/')[1]
            #             for o in objs if getattr(getattr(o,outer),inner,None)
            #         )
            #     else:
            #         # print(getattr(o, f).url)
            #         # if getattr(o,f):
            #         #     #print(getattr(o, f).url.split(fromMountUrl)[1])
            #         #     #print('/'+getattr(o, f).url.replace(fromMountUrl,toMountUrl).split(prefix)[1])
            #         #     print( getattr(o, f).url.replace(fromMountUrl,toMountUrl).split(prefix+toContainer+'/')[1])
                    
            #         for o in objs:
            #             if getattr(o,f):
            #                 print(f"frommy: {getattr(o, f).url}")
            #                 toUrls.append(
            #                     getattr(o, f).url.replace(fromMountUrl,toMountUrl).split(prefix+toContainer+'/')[1]
            #                 )
            #             else:
            #                 toUrls.append(None)
            #         # toUrls.append(
            #         #     #getattr(o, f).url.replace(fromMountUrl,toMountUrl).split(toContainer+'/')[1]
            #         #     #getattr(o, f).url.split(fromMountUrl)[1]
            #         #     #getattr(o, f).url.replace(fromMountUrl,toMountUrl)
            #         #     #'/'+getattr(o, f).url.replace(fromMountUrl,toMountUrl).split(prefix)[1]
            #         #     getattr(o, f).url.replace(fromMountUrl,toMountUrl).split(prefix+toContainer+'/')[1]
                        
            #         #     #self.toFS.url(getattr(o, f).path)
            #         #     for o in objs if getattr(o, f,None)
            #         # )
            # # old code

            # # toFiles = [
            # #     getattr(o, field).url.replace(fromMountUrl,toMountUrl).split(prefix+toContainer+'/')[1]
            # #     for o in objs
            # #     for field in file_fields if getattr(o, field, None)
            # # ]
            
            # # print(f"Copying {len(fromSignedUrls)} files...",end="")
            # # if not dry_run:
            # #     self.toFS.copyFiles(fromSignedUrls,toFiles)
            # # print(" Done.")
            # # if remove_file_keys: # Nullifies a field in the destination
            # #     for o in objs:
            # #         for field in remove_file_keys:
            # #             setattr(o, field, None)
            # # if reset_version:
            # #     for o in objs:
            # #         o.version = None
            # # print(fromSignedUrls)
            # # print(toUrls)
            # # if not dry_run:
            # #     self.tryMergeBatch(self.c3_to, typeName, objs)
            # # if single_batch:
            # #     break
            # # offset += batch_size
            # # batch += 1

            # Copy files
            if dry_run:
                # print(len(fromSignedUrls))
                print(len(toUrls))
            else:
                print(f"Copying {len(fromSignedUrls)} files...")
                # self.copyObj.copyUrls(fromSignedUrls, toFiles)
                self.copyObj.copyUrls(fromSignedUrls, toUrls)

                print("Done copying files.")
    # Update c3 objects with new urls and metadata
            if update_metadata:
                # toResult = self.tryFetch(self.c3_to, typeName, spec)
                # toObjs = toResult.objs
                print("Update Metadata")
                #updatedObjFiles = self.tryUpdateFileMetadata(self.c3_to, objs, file_fields)
                objFiles = {}
                # for field in file_fields:
                #print(f"Migrating {typeName}.{field}")
                for cntr,f in enumerate(file_fields):
                    if len(f.split('.')) > 1:
                        outer,inner = f.split('.')
                        for o in objs:
                            if getattr(getattr(o,outer),inner):
                                objFiles[str(o.id)] = {
                                    'obj': o,
                                    'url': getattr(getattr(o,outer),inner).url.split(fromMountUrl)[1]
                                }
                    else:
                        for o in objs:
                            if getattr(o,f):
                                objFiles[str(o.id)] = {
                                    'obj': o,
                                    'url': getattr(o, f).url.split(fromMountUrl)[1]
                                }
                    if dry_run:
                        print(objFiles)

                    #updatedObjFiles = self.tryUpdateFileMetadata(self.c3_to, objFiles, f, remove_file_keys=remove_file_keys,url_only=True)
                    updatedObjFiles = self.tryUpdateFileMetadata(self.c3_to, objFiles, f, remove_file_keys=remove_file_keys,url_only=url_only)
                    # if updatedObjFiles:
                    #     if url_only:
                    #         toResult = self.tryFetch(self.c3_to, typeName, spec)
                    #         if toResult:
                    #             print("oh yeah...")
                    #             toObjs = toResult.objs
                    #             for o in toObjs:
                    #                 o.version = None
                    #                 setattr(o,field, getattr(o,field).fs().listFiles(getattr(o,field).url).files[0].toJson())
                    #             getattr(self.c3_to,typeName).mergeBatch([ o.toJson() for o in toObjs])
                    # else:
                    if not updatedObjFiles:
                        updatedObjFiles = {}
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
                    else:
                        print(f"Comparing source and dest file size and MD5")
                        for id,oDict in updatedObjFiles.items():
                            #print(oDict['url'])
                            toObj = oDict['obj']
                            fromObj = objFiles[id]['obj']
                            toLen = getattr(toObj, f).contentLength
                            fromLen = getattr(fromObj, f).contentLength
                            toMD5 = getattr(toObj, f).contentMD5
                            fromMD5 = getattr(fromObj, f).contentMD5
                            assert toLen == fromLen, f"{toLen} != {fromLen} for {toObj.id}"
                            assert toMD5 == fromMD5, f"{toMD5} != {fromMD5} for {toObj.id}"           
                
            if single_batch:
                break
            offset += batch_size
            batch+=1 