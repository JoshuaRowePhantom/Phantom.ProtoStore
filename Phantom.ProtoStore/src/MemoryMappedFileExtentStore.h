#include "ExtentStore.h"
#include <filesystem>

namespace Phantom::ProtoStore
{

class MemoryMappedFileExtentStore
    :
    public IExtentStore
{
    Schedulers m_schedulers;
    std::string m_extentFilenamePrefix;
    std::string m_extentFilenameSuffix;
    uint64_t m_writeBlockSize;

    std::string GetFilename(
        ExtentName extentName);

public:
    MemoryMappedFileExtentStore(
        Schedulers schedulers,
        std::string extentFilenamePrefix,
        std::string extentFilenameSuffix,
        uint64_t writeBlockSize
    );
    
    task<shared_ptr<IReadableExtent>> OpenExtentForRead(
        std::filesystem::path path
    );

    virtual task<shared_ptr<IReadableExtent>> OpenExtentForRead(
        ExtentName extentName
    ) override;

    virtual task<shared_ptr<IWritableExtent>> OpenExtentForWrite(
        ExtentName extentName
    ) override;

    virtual task<> DeleteExtent(
        ExtentName extentName
    ) override;
};

}
