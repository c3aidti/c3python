from azure.storage.blob import (
    BlobServiceClient,
    generate_account_sas, ResourceTypes, AccountSasPermissions,
    generate_blob_sas, BlobSasPermissions,
)

import time
from datetime import datetime, timedelta

class CopyUrlsToContainer:
    def __init__(self,toContainer,toConnectionString):
        self.toContainer = toContainer
        #self.toConnectionString = toConnectionString
        self.toBlobServiceClient = BlobServiceClient.from_connection_string(toConnectionString)
        # print(toConnectionString)
        # print(toContainer)
        # print("containers:")
        for c in self.toBlobServiceClient.list_containers():
            print(c['name'])

    def copyUrls(self, fromUrlList, toUrlList, wait=True):
        toBlobs = []
        stats = []
        for i, fromUrl in enumerate(fromUrlList):
            # print(f"1: {fromUrl}")
            # print(f"2: {toUrlList[i]}")
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
            print("Copy complete.")

    # def copyUrlBatches(self, fromUrlList, toUrlList, batch_size=2000, wait=True, wait_time=60):
    #     # compute total number of batches
    #     num_batches = len(fromUrlList) // batch_size

    #     for i in range(0, len(fromUrlList), batch_size):
    #         toBlobs = []
    #         stats = []
    #         batch_from_urls = fromUrlList[i:i+batch_size]
    #         batch_to_urls = toUrlList[i:i+batch_size]
    #         # Report progress: starting batch n of num_batchees
    #         print(f"Starting batch {str(i//batch_size)} of {str(num_batches)}")

    #         for j, fromUrl in enumerate(batch_from_urls):
    #             to_blob = self.toBlobServiceClient.get_blob_client(self.toContainer, batch_to_urls[j])
    #             to_blob.start_copy_from_url(fromUrl)
    #             toBlobs.append(to_blob)
    #             stats.append('started')

    #         def checkStats(toBlobs, stats):
    #             cnt = 0
    #             for i, to_blob in enumerate(toBlobs):
    #                 if stats[i] != "success":
    #                     props = to_blob.get_blob_properties()
    #                     status = props.copy.status
    #                     stats[i] = status
    #                     if status == "success":
    #                         cnt += 1
    #                 else:
    #                     cnt += 1
    #             return cnt
    def copyUrlBatches(self, fromUrlList, toUrlList, batch_size=2000, wait=True, wait_time=60, overwrite=False):
        # compute total number of batches
        num_batches = len(fromUrlList) // batch_size

        for i in range(0, len(fromUrlList), batch_size):
            toBlobs = []
            stats = []
            batch_from_urls = fromUrlList[i:i+batch_size]
            batch_to_urls = toUrlList[i:i+batch_size]
            # Report progress: starting batch n of num_batchees
            print(f"Starting batch {str(i//batch_size)} of {str(num_batches)}")

            for j, fromUrl in enumerate(batch_from_urls):
                to_blob = self.toBlobServiceClient.get_blob_client(self.toContainer, batch_to_urls[j])
                if to_blob.exists() and not overwrite:
                    # print(f"Blob {to_blob.blob_name} already exists. Skipping copy operation.")
                    stats.append('success')
                else:
                    to_blob.start_copy_from_url(fromUrl)
                    toBlobs.append(to_blob)
                    stats.append('started')

            # for j, fromUrl in enumerate(batch_from_urls):
            #     to_blob = self.toBlobServiceClient.get_blob_client(self.toContainer, batch_to_urls[j])
            #     if to_blob.exists():
            #         props = to_blob.get_blob_properties()
            #         if props.copy and props.copy.status == 'pending':
            #             print(f"Blob {to_blob.blob_name} has a pending copy operation. Skipping.")
            #             stats.append('pending')
            #         else:
            #             to_blob.start_copy_from_url(fromUrl)
            #             toBlobs.append(to_blob)
            #             stats.append('started')
            #     else:
            #         to_blob.start_copy_from_url(fromUrl)
            #         toBlobs.append(to_blob)
            #         stats.append('started')

            # for j, fromUrl in enumerate(batch_from_urls):
            #     to_blob = self.toBlobServiceClient.get_blob_client(self.toContainer, batch_to_urls[j])
            #     if to_blob.exists():
            #         props = to_blob.get_blob_properties()
            #         if props.copy and props.copy.status == 'pending':
            #             print(f"Blob {to_blob.blob_name} has a pending copy operation. Attempting to delete.")
            #             try:
            #                 to_blob.delete_blob()
            #                 print(f"Deleted blob {to_blob.blob_name}.")
            #             except Exception as e:
            #                 print(f"Failed to delete blob {to_blob.blob_name}. Error: {str(e)}")
            #             stats.append('deleted')
            #         else:
            #             # to_blob.start_copy_from_url(fromUrl)
            #             toBlobs.append(to_blob)
            #             stats.append('started')
            #     else:
            #         # to_blob.start_copy_from_url(fromUrl)
            #         toBlobs.append(to_blob)
            #         stats.append('started')


            def checkStats(toBlobs, stats):
                cnt = 0
                for i, to_blob in enumerate(toBlobs):
                    if stats[i] == 'success':
                        cnt += 1
                    else:
                        props = to_blob.get_blob_properties()
                        status = props.copy.status
                        stats[i] = status
                        if status == "success":
                            cnt += 1
                return cnt

            if wait:
                doneCnt = 0
                time.sleep(wait_time)
                while doneCnt < len(toBlobs) - 1:
                    doneCnt = checkStats(toBlobs, stats)
                    print(f"Copy status: {str(doneCnt)} of {str(len(toBlobs))} complete.")
                    if doneCnt < len(toBlobs):
                        time.sleep(wait_time)
                print(f"Batch complete. {str(doneCnt)} of {str(len(toBlobs))} files copied successfully.")

        # if wait:
        #     doneCnt = 0
        #     while doneCnt < len(toBlobs) - 1:
        #         doneCnt = checkStats(toBlobs, stats)
        #         print(f"Copy status: {str(doneCnt)} of {str(len(toBlobs))} complete.")
        #         if doneCnt < len(toBlobs):
        #             time.sleep(12)
        print("Copy complete.")

# def GetBlobs(fromBlobContainer, fromConnectionString, prefix):
#     fromBlobServiceClient = BlobServiceClient.from_connection_string(fromConnectionString)
#     fromContainerClient = fromBlobServiceClient.get_container_client(fromBlobContainer)
#     blobs = []
#     for blob in fromContainerClient.list_blobs(name_starts_with=prefix):
#         blobs.append(blob)
#     return blobs
def GetBlobs(fromBlobContainer, fromConnectionString, prefix):
    fromBlobServiceClient = BlobServiceClient.from_connection_string(fromConnectionString)
    fromContainerClient = fromBlobServiceClient.get_container_client(fromBlobContainer)
    blobs = []
    blob_iterable = fromContainerClient.list_blobs(name_starts_with=prefix).by_page()
    for blob_page in blob_iterable:
        for blob in blob_page:
            blobs.append(blob)
    return blobs

# Provide list of blobs to copy with geven prefix and append an account level SAS token to each blob
# return list of signed urls
# def GetSignedUrls(fromBlobContainer, fromConnectionString, prefix):
#     # get an account level SAS token, equvalent of:
#     # az storage container generate-sas \
#     # --account-name $SOURCE_ACCOUNT_NAME \
#     # --name $SOURCE_CONTAINER_NAME \
#     # --connection-string $SOURCE_CONNECTION_STRING \
#     # --permissions r \
#     # --expiry $(date -u -d "+1 day" '+%Y-%m-%dT%H:%M:%SZ') \
#     # --https-only -o tsv)

#     fromBlobServiceClient = BlobServiceClient.from_connection_string(fromConnectionString)
#     fromContainerClient = fromBlobServiceClient.get_container_client(fromBlobContainer)
#     sas_token = fromContainerClient.generate_shared_access_signature(
#         permission=ContainerSasPermissions(read=True),
#         expiry=datetime.utcnow() + timedelta(hours=1),
#         https_only=True
#     )
#     # list blobs with prefix and append SAS token
#     blobs = []
#     blob_iterable = fromContainerClient.list_blobs(name_starts_with=prefix).by_page()
#     for blob_page in blob_iterable:
#         for blob in blob_page:
#             blobs.append(blob.url + "?" + sas_token)
#     return blobs
def GetSignedUrls(fromBlobContainer, fromConnectionString, prefix):
    fromBlobServiceClient = BlobServiceClient.from_connection_string(fromConnectionString)
    fromContainerClient = fromBlobServiceClient.get_container_client(fromBlobContainer)
    account_sas = AccountSasPermissions(read=True),
    sas_token = generate_account_sas(
        account_name=fromBlobServiceClient.account_name,
        account_key=fromBlobServiceClient.credential.account_key,
        resource_types=ResourceTypes(container=True, object=True),
        permission=account_sas,
        expiry=datetime.utcnow() + timedelta(hours=1),
        start=datetime.utcnow() - timedelta(minutes=1),
        https_only=True
    )
    blobs = []
    blob_iterable = fromContainerClient.list_blobs(name_starts_with=prefix).by_page()
    for blob_page in blob_iterable:
        for blob in blob_page:
            blob_client = fromContainerClient.get_blob_client(blob.name)
            blobs.append(blob_client.url + "?" + sas_token)
    return blobs

# Function to combine the above functions and return a list of signed urls and a list of destination urls
# the destination urls are just the blob names with the prefix removed
# def GetSignedUrlsAndDestinations(fromBlobContainer, fromConnectionString, prefix):
#     fromBlobServiceClient = BlobServiceClient.from_connection_string(fromConnectionString)
#     fromContainerClient = fromBlobServiceClient.get_container_client(fromBlobContainer)
#     account_sas = AccountSasPermissions(read=True),
#     sas_token = generate_account_sas(
#         account_name=fromBlobServiceClient.account_name,
#         account_key=fromBlobServiceClient.credential.account_key,
#         resource_types=ResourceTypes(container=True, object=True),
#         permission=account_sas,
#         expiry=datetime.utcnow() + timedelta(hours=24),
#         start=datetime.utcnow() - timedelta(minutes=1),
#         https_only=True
#     )
#     print(sas_token)
#     blobs = []
#     destinations = []
#     blob_iterable = fromContainerClient.list_blobs(name_starts_with=prefix).by_page()
#     for blob_page in blob_iterable:
#         for blob in blob_page:
#             blob_client = fromContainerClient.get_blob_client(blob.name)
#             blobs.append(blob_client.url + "?" + sas_token)
#             #destinations.append(blob.name[len(prefix):])
#             destinations.append(blob.name)
#     return blobs, destinations

def GetSignedUrlsAndDestinations(fromBlobContainer, fromConnectionString, prefix):
    fromBlobServiceClient = BlobServiceClient.from_connection_string(fromConnectionString)
    fromContainerClient = fromBlobServiceClient.get_container_client(fromBlobContainer)
    blobs = []
    destinations = []
    blob_iterable = fromContainerClient.list_blobs(name_starts_with=prefix).by_page()
    for blob_page in blob_iterable:
        for blob in blob_page:
            blob_client = fromContainerClient.get_blob_client(blob.name)
            sas_token = generate_blob_sas(
                account_name=fromBlobServiceClient.account_name,
                container_name=fromBlobContainer,
                blob_name=blob.name,
                account_key=fromBlobServiceClient.credential.account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(hours=72),
            )
            blobs.append(blob_client.url + "?" + sas_token)
            destinations.append(blob.name)
    return blobs, destinations