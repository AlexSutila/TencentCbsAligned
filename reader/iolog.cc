#include <iostream>
#include <cstdint>
#include <fstream>

int main(int argc, char* argv[])
{
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <input_file>\n";
        return 1;
    }

    std::ifstream file(argv[1], std::ios::binary);
    if (!file) {
        std::cerr << "Error opening file: " << argv[1] << "\n";
        return 1;
    }

    std::cout << "fio version 2 iolog\n"
        << "/dev/mapper/dm_foo add\n"
        << "/dev/mapper/dm_foo open\n";

    while (true) {
        std::uint64_t offset;
        std::uint32_t size;
        std::uint8_t io_type;

        file.read(reinterpret_cast<char*>(&offset), sizeof(offset));
        file.read(reinterpret_cast<char*>(&size), sizeof(size));
        file.read(reinterpret_cast<char*>(&io_type), sizeof(io_type));
        if (!file) break;

        std::cout << "/dev/mapper/dm_foo "
            << (io_type ? "write " : "read ")
            << (offset * 512) << " "
            << size << "\n";
    }

    std::cout << "/dev/mapper/dm_foo close" << std::endl;
    return 0;
}
