using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Maincotech.Storage.AzureBlob
{
    public class AzureBlobStorage : IBlobStorage
    {
        private readonly string _connectionString;

        public AzureBlobStorage(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task<BlobContainer> CreateContainer(BlobContainer container)
        {
            var blobServiceClient = new BlobServiceClient(_connectionString);
            var publicAcessType = PublicAccessType.None;
            if (container.Metadata?.Any(x => x.Key == "PublicAccessType") == true)
            {
                publicAcessType = Enum.Parse<PublicAccessType>(container.Metadata["PublicAccessType"].ToString());
            }
            IDictionary<string, string> metadata = null;
            if ((container.Metadata?.Count > 0) == true)
            {
                metadata = new Dictionary<string, string>();
                foreach (var prop in container.Metadata)
                {
                    if (prop.Key != "PublicAccessType")
                    {
                        metadata.Add(prop.Key, prop.Value.ToString());
                    }
                }
            }
            var response = (await blobServiceClient.CreateBlobContainerAsync(container.Identifier, publicAcessType, metadata)).Value;
            return ConstructContainer(response);
        }

        /// <summary>
        /// Delete blob by Uri
        /// </summary>
        /// <param name="id">The Blob Uri</param>
        /// <returns></returns>
        public async Task DeleteBlob(string id)
        {
            var blobClient = new BlobClient(new Uri(id));
            var containerName = blobClient.BlobContainerName;
            var blobName = blobClient.Name;
            blobClient = new BlobClient(_connectionString, containerName, blobName);
            await blobClient.DeleteIfExistsAsync();
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="id">The container name</param>
        /// <returns></returns>
        public async Task DeleteContainer(string id)
        {
            var blobServiceClient = new BlobServiceClient(_connectionString);
            await blobServiceClient.DeleteBlobContainerAsync(id);
        }

        public async Task<bool> Exists(Blob blob)
        {
            var blobClient = new BlobClient(new Uri(blob.Identifier));
            var containerName = blobClient.BlobContainerName;
            var blobName = blobClient.Name;
            blobClient = new BlobClient(_connectionString, containerName, blobName);
            return (await blobClient.ExistsAsync()).Value;
        }

        public async Task<Blob> GetBlob(string id)
        {
            var blobClient = new BlobClient(new Uri(id));
            var containerName = blobClient.BlobContainerName;
            var blobName = blobClient.Name;
            blobClient = new BlobClient(_connectionString, containerName, blobName);
            return ConstructBlob(blobClient);
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="id">Container name</param>
        /// <returns></returns>
        public async Task<BlobContainer> GetBlobContainerById(string id)
        {
            return ConstructContainer(new BlobContainerClient(_connectionString, id));
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="path">Container name</param>
        /// <returns></returns>
        public async Task<BlobContainer> GetBlobContainerByPath(string path)
        {
            return ConstructContainer(new BlobContainerClient(_connectionString, path));
        }

        public async Task<IList<Blob>> GetBlobs(BlobContainer container)
        {
            var result = new List<Blob>();
            // Get a reference to a container named "sample-container" and then create it
            var blobContainerClient = new BlobContainerClient(_connectionString, container.Name);
            await foreach (var blob in blobContainerClient.GetBlobsAsync())
            {
                result.Add(ConstructBlob(blob));
            }            
            return result;
        }

        /// <summary>
        /// The Azure Storage does support containers inside a container
        /// </summary>
        /// <param name="container"></param>
        /// <returns></returns>
        public Task<IList<BlobContainer>> GetContainers(BlobContainer container)
        {
            throw new NotImplementedException();
        }

        public Task<string> GetOwner()
        {
            throw new NotImplementedException();
        }

        public Task<Quota> GetQuota()
        {
            throw new NotImplementedException();
        }

        public async Task<Stream> OpenBlob(Blob blob)
        {
            var blobClient = new BlobClient(new Uri(blob.Uri));
            var containerName = blobClient.BlobContainerName;
            var blobName = blobClient.Name;
            blobClient = new BlobClient(_connectionString, containerName, blobName);
            var downloadInfo = (await blobClient.DownloadAsync()).Value;
            return downloadInfo.Content;
        }

        public async Task RenameBlob(string id, string newName, string displayName)
        {
            var blobClient = new BlobClient(new Uri(id));
            var containerName = blobClient.BlobContainerName;
            var blobName = blobClient.Name;
            blobClient = new BlobClient(_connectionString, containerName, blobName);

            var targetBlobClient = new BlobClient(_connectionString, containerName, newName);
            await targetBlobClient.SyncCopyFromUriAsync(new Uri(id));
            await blobClient.DeleteAsync();
        }

        public Task RenameContainer(string id, string newName, string displayName)
        {
            throw new NotImplementedException();
        }

        public async Task<Blob> SaveBlob(Blob blob, Stream content)
        {
            var blobClient = new BlobClient(new Uri(blob.Identifier));
            var containerName = blobClient.BlobContainerName;
            var blobName = blobClient.Name;
            blobClient = new BlobClient(_connectionString, containerName, blobName);
            await blobClient.UploadAsync(content);
            return ConstructBlob(blobClient);
        }

        private BlobContainer ConstructContainer(BlobContainerClient item)
        {
            var result = new BlobContainer
            {
                BlobStorage = this,
                DisplayName = item.Name,
                Name = item.Name,
                Identifier = item.Name,
                Uri = item.Uri.ToString(),
                Metadata = new PropertyBag
                {
                    { "AccountName", item.AccountName },
                }
            };

            return result;
        }

        private Blob ConstructBlob(BlobClient item)
        {
            var result = new Blob
            {
                BlobStorage = this,
                Name = item.Name,
                Identifier = item.Uri.ToString(),
                Uri = item.Uri.ToString(),
                //Size = item.,
                Parent = ConstructContainer(new BlobContainerClient(_connectionString, item.BlobContainerName)),
                Metadata = new PropertyBag
                {
                    { "AccountName", item.AccountName },
                    { "ContainerName", item.BlobContainerName },
                }
            };

            return result;
        }

        private Blob ConstructBlob(BlobItem item)
        {
            var metadata = new PropertyBag
            {
            };
            if (item.Metadata?.Count > 0 == true)
            {
                foreach (var prop in item.Metadata)
                {
                    metadata.Add(prop.Key, prop.Value);
                }
            }
            var result = new Blob
            {
                BlobStorage = this,
                Name = item.Name,
                Identifier = item.Name,
                // Uri = item.Uri.ToString(),
                //Size = item.,
                //  Parent = ConstructContainer(new BlobContainerClient(_connectionString, item.BlobContainerName)),
                Metadata = metadata
            };

            return result;
        }
    }
}