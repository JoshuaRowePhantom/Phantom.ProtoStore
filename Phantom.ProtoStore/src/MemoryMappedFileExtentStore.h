#include "ExtentStore.h"

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
        ExtentNumber extentNumber);

public:
    MemoryMappedFileExtentStore(
        Schedulers schedulers,
        std::string extentFilenamePrefix,
        std::string extentFilenameSuffix,
        uint64_t writeBlockSize
    );

    virtual task<shared_ptr<IReadableExtent>> OpenExtentForRead(
        ExtentNumber extentNumber
    ) override;

    virtual task<shared_ptr<IWritableExtent>> OpenExtentForWrite(
        ExtentNumber extentNumber
    ) override;

    virtual task<> DeleteExtent(
        ExtentNumber extentNumber
    ) override;
};

}
