#include "ExtentStore.h"
#include <filesystem>

namespace Phantom::ProtoStore
{

class MemoryMappedFileExtentStore
    :
    public IExtentStore
{
public:
    enum class ExtentDeleteAction
    {
        Rename,
        Delete,
    };

private:
    const Schedulers m_schedulers;
    const std::string m_extentFilenamePrefix;
    const std::string m_extentFilenameSuffix;
    const uint64_t m_writeBlockSize;
    const ExtentDeleteAction m_extentDeleteAction;

    std::string GetSanitizedIndexName(
        std::string_view indexName);

    std::string GetFilename(
        const ExtentName* extentName);

public:
    MemoryMappedFileExtentStore(
        Schedulers schedulers,
        std::string extentFilenamePrefix,
        std::string extentFilenameSuffix,
        uint64_t writeBlockSize,
        ExtentDeleteAction extentDeleteAction
    );
    
    task<shared_ptr<IReadableExtent>> OpenExtentForRead(
        std::filesystem::path path
    );

    task<shared_ptr<IReadableExtent>> OpenExtentForRead(
        std::filesystem::path path,
        const ExtentName* extentName
    );

    virtual task<shared_ptr<IReadableExtent>> OpenExtentForRead(
        const FlatBuffers::ExtentName* extentName
    ) override;

    virtual task<shared_ptr<IWritableExtent>> OpenExtentForWrite(
        const FlatBuffers::ExtentName* extentName
    ) override;

    virtual task<> DeleteExtent(
        const FlatBuffers::ExtentName* extentName
    ) override;
};

}
