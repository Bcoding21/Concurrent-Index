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

class BlockIndexer
{
	static unsigned long termID, linkID;
	static std::string outDir;
	static std::unordered_map<std::string, unsigned long> termDict, docDict;
	std::map<unsigned long, std::vector<unsigned long>> blockIndex;
	static std::queue<std::string> blockQueue;

	static std::pair<unsigned long, std::vector<unsigned long>> get(std::ifstream&);
	static void merge(const std::string&, const std::string&, const std::string&);
	static int getIndexSizeFromFile(std::ifstream&);
	static void put(const std::pair<unsigned long, std::vector<unsigned long>>&, std::ofstream&);
	static void storeTermDict(std::string = "termDict.bin");
	static void storeDocDict(std::string = "docDict.bin");
	static std::unordered_map<unsigned long, std::vector<unsigned long>> readIndex(const std::string&);
	void storeBlockIndex(const std::string&);

public:
	BlockIndexer();
	BlockIndexer(const BlockIndexer&);
	void index(const std::string&);
	static void mergeIndexes();
};

