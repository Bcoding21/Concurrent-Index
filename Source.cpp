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

std::mutex termMu, linkMu, queueMu, queueMu2;
namespace fs = std::experimental::filesystem::v1;


struct Dictionary {
	// maps terms to unque term id
	std::unordered_map<std::string, unsigned long> termDict;

	// maps file to unique file id
	std::unordered_map<std::string, unsigned long> fileDict;

	// uniquely identifies each file
	unsigned long fileCounter = 0;

	// uniquely identifies each term
	unsigned long termCounter = 0;
};


void indexData(std::queue<fs::directory_entry>&, Dictionary&, const std::string&);

void storeIndex(const std::map<unsigned long, std::set<unsigned long>>&, const std::string&);

void storeDictionary(const std::unordered_map<std::string, unsigned long>& dict, const std::string& path);

int main() {

	std::string inDir = "C:\\Users\\Brandon\\Documents\\Projects\\C++\\MultiThreadedIndexer\\pa1-data\\pa1-data";

	std::queue<fs::directory_entry> dirEntryQueue;

	for (const auto& subDir : fs::directory_iterator(inDir)) {
		dirEntryQueue.push(subDir);
	}
	
	short numThreads = std::thread::hardware_concurrency();

	std::vector<std::thread> threads;
	threads.reserve(numThreads);

	Dictionary dictionary;

	std::string outDir = "C:\\Users\\Brandon\\Documents\\Projects\\C++\\MultiThreadedIndexer\\";

	for (int i = 0; i < numThreads - 1; i++) {

		threads.push_back(std::thread(&indexData, std::ref(dirEntryQueue), std::ref(dictionary), std::ref(outDir)));

	}

	indexData(dirEntryQueue, dictionary, outDir);

	
	for (auto& thread : threads) {
		thread.join();
	}


}


void indexData(std::queue<fs::directory_entry>& dirQueue, Dictionary& index, const std::string& outDir) {

	fs::directory_entry entry;

	while (!dirQueue.empty()) {

		{
			std::lock_guard<std::mutex> locker(queueMu2);
			entry = dirQueue.front();
			dirQueue.pop();
		}

		std::map<unsigned long, std::set<unsigned long>> termIdIndex;

		for (const auto& file : fs::directory_iterator(entry)) {

			std::string fileName = fs::path(file).filename().string();
			unsigned long fileId;

			{
				std::lock_guard<std::mutex> locker(linkMu);
				auto it = index.fileDict.emplace(fileName, index.fileCounter);
				index.fileCounter += it.second;
				fileId = it.first->second;
			}

			std::ifstream stream(file);
			std::istream_iterator<std::string> start(stream), end;
			std::vector<std::string> terms(start, end);

			for (const auto& term : terms) {

				unsigned long termId;

				{
					std::lock_guard<std::mutex> lock(termMu);
					auto it = index.termDict.emplace(term, index.termCounter);
					index.termCounter += it.second;
					termId = it.first->second;
				}

				auto newFileIdList = { fileId };
				auto iter = termIdIndex.emplace(termId, newFileIdList);
				bool containsId = !iter.second;

				if (containsId) {
					iter.first->second.emplace(fileId);
				}
			}
		}

		std::string outPath = outDir + fs::path(entry).filename().string();
		storeIndex(termIdIndex, outPath);
	}
}

void storeIndex(const std::map<unsigned long, std::set<unsigned long>>& termIdIndex, const std::string& path ) {

	std::ofstream stream(path, std::ios::binary);

	if (stream.is_open()) {

		int indexSize = termIdIndex.size();
		stream.write(reinterpret_cast<char*>(&indexSize), sizeof(indexSize));

		for (const auto& entry : termIdIndex) {

			stream.write(reinterpret_cast<const char*>(&entry.first), sizeof(entry.first));
			int setSize = entry.second.size();
			stream.write(reinterpret_cast<char*>(&setSize), sizeof(setSize));

			for (const auto& fileId : entry.second) {
				stream.write(reinterpret_cast<const char*>(&fileId), sizeof(fileId));
			}
		}
	}
}

void storeDictionary(const std::unordered_map<std::string, unsigned long>& dict, const std::string& path) {

	

}
