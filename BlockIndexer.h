#pragma once
#include <unordered_map>
#include <map>
#include <set>
#include <string>
#include <iostream>
#include <atomic>
#include <queue>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <mutex>
#include <algorithm>

namespace fs = std::experimental::filesystem::v1;

class BlockIndexer
{

	static std::string outDir;
	static std::unordered_map<std::string, unsigned long> docDict;
	static std::deque<std::pair<std::string, unsigned long>> termDict;
	static std::queue<std::string> blockQueue;

	static std::pair<unsigned long, std::vector<unsigned long>> get(std::ifstream&);
	static void merge(const std::string&, const std::string&, const std::string&);
	static int getIndexSizeFromFile(std::ifstream&);
	static void put(const std::pair<unsigned long, std::vector<unsigned long>>&, std::ofstream&);
	static void storeTermDict(std::string = "termDict.bin");
	static void storeDocDict(std::string = "docDict.bin");
	static std::unordered_map<unsigned long, std::vector<unsigned long>> readIndex(const std::string&);
	void storeBlockIndex(const std::string&, std::map<unsigned long, std::vector<unsigned long>>& blockIndex);

public:
	BlockIndexer();
	BlockIndexer(const BlockIndexer&);
	void index(std::queue<std::string>&);
	static void mergeIndexes();
	static unsigned long termID, linkID;
};

