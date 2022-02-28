from azure.storage.blob import (
    BlobServiceClient
)

import time

class CopyUrlsToContainer:
    def __init__(self,toContainer,toConnectionString):
        self.toContainer = toContainer
        #self.toConnectionString = toConnectionString
        self.toBlobServiceClient = BlobServiceClient.from_connection_string(toConnectionString)
        # print(toConnectionString)
        # print(toContainer)
        # print("containers:")
        # for c in self.toBlobServiceClient.list_containers():
        #     print(c['name'])

    def copyUrls(self, fromUrlList, toUrlList, wait=True):
        toBlobs = []
        stats = []
        for i, fromUrl in enumerate(fromUrlList):
            # print(fromUrl)
            # print(toUrlList[i])
            to_blob = self.toBlobServiceClient.get_blob_client(self.toContainer, toUrlList[i])
            to_blob.start_copy_from_url(fromUrl)
            toBlobs.append(to_blob)
            stats.append('started')

        def checkStats(toBlobs,stats):
            cnt=0
            for i,to_blob in enumerate(toBlobs):
                    props = to_blob.get_blob_properties()
                    status = props.copy.status
                    stats[i] = status
                    if status == "success":
                        cnt += 1
            return cnt
        if wait:
            doneCnt=0
            while doneCnt < len(toBlobs)-1:
                doneCnt = checkStats(toBlobs,stats)
                print(f"Copy status: {str(doneCnt)} of {str(len(toBlobs))} complete.")
                if doneCnt < len(toBlobs):
                    time.sleep(12)
