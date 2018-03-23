#include "BlockIndexer.h"

std::mutex termMu, linkMu, queueMu, queueMu2;

unsigned long BlockIndexer::termID = 0, BlockIndexer::linkID = 0;

std::string BlockIndexer::outDir = "C:\\Users\\Brandon\\source\\repos\\Query\\Query\\";

std::unordered_map<std::string, unsigned long> BlockIndexer::docDict = {};

std::map<std::string, unsigned long> BlockIndexer::termDict = {};

std::queue<std::string> BlockIndexer::blockQueue = {};

BlockIndexer::BlockIndexer() {}

BlockIndexer::BlockIndexer(const BlockIndexer& indexer)  {}

void BlockIndexer::index(std::queue<std::string>& dirQueue) { 

	std::string dir;
	while (!dirQueue.empty()) {
		{
			std::lock_guard<std::mutex> locker(queueMu2);
			dir = dirQueue.front();
			dirQueue.pop();
		}

		std::map<unsigned long, std::vector<unsigned long>> blockIndex;

		for (const auto& filePath : fs::directory_iterator(dir)) {

			std::string link = fs::path(filePath).filename().string();

			unsigned long currLinkID;
			{
				std::lock_guard<std::mutex> locker(linkMu);
				auto good = docDict.emplace(link, linkID);
				linkID += good.second;
				currLinkID = good.first->second;
			}

			std::ifstream file(filePath);
			std::string term;

			while (file >> term) {

				unsigned long currTermID;
				auto termSearch = termDict.find(term);

				if (termSearch == termDict.end()) {
					std::lock_guard<std::mutex> lock(termMu);
					auto ok = termDict.emplace_hint(termDict.end(), term, termID);
					currTermID = termID++;
				}
				else {
					currTermID = termSearch->second;
				}

				if (blockIndex.find(currTermID) == blockIndex.end()) {
					auto list = { currLinkID };
					blockIndex.emplace_hint(blockIndex.end(), currTermID, list);
				}
				else {
					blockIndex.at(currTermID).push_back(currLinkID);
				}
			}
		}

		std::string outPath = outDir + fs::path(dir).filename().string();
		storeBlockIndex(outPath, blockIndex);

		std::lock_guard<std::mutex> locker(queueMu);
		blockQueue.push(outPath);

	}
}

void BlockIndexer::storeBlockIndex(const std::string& path, std::map<unsigned long, std::vector<unsigned long>>& blockIndex) {

	std::ofstream out(path, std::ofstream::binary);

	for (auto& pair : blockIndex){

		std::sort(pair.second.begin(), pair.second.end());

		out.write(reinterpret_cast<const char*>(&pair.first), sizeof(pair.first));

		unsigned int size = pair.second.size();

		out.write(reinterpret_cast<char*>(&size), sizeof(size));

		for (auto num : pair.second) {

			out.write(reinterpret_cast<char*>(&num), sizeof(num));

		}
	}

	unsigned int size = blockIndex.size();
	out.write(reinterpret_cast<char*>(&size), sizeof(size));
}

void BlockIndexer::mergeIndexes() {

	while (true) {
		
		if (blockQueue.size() <= 2) {

			auto file1 = blockQueue.front();
			blockQueue.pop();

			auto file2 = blockQueue.front();
			blockQueue.pop();

			auto mergedFile = outDir + "index.bin";
			merge(file1, file2, mergedFile);

			fs::remove(file1);
			fs::remove(file2);
			break;
		}

		auto file1 = blockQueue.front();
		blockQueue.pop();

		auto file2 = blockQueue.front();
		blockQueue.pop();

		auto fileName1 = fs::path(file1).filename().string();

		auto fileName2 = fs::path(file2).filename().string();

		auto mergedFile = outDir + fileName1 + fileName2;

		merge(file1, file2, mergedFile);

		blockQueue.push(mergedFile);

		fs::remove(file1);
		fs::remove(file2);
	}

	storeTermDict();
	storeDocDict();
}

void BlockIndexer::merge(const std::string& file1, const std::string& file2, const std::string& mergedFile) {

	std::ifstream f1(file1, std::ifstream::binary), f2(file2, std::ifstream::binary);
	std::ofstream f3(mergedFile, std::ofstream::binary);

	if (f1 && f2 && f3) {
		unsigned int firstSize = getIndexSizeFromFile(f1);
		unsigned int secondSize = getIndexSizeFromFile(f2);

		auto pair1 = get(f1);
		unsigned int i = 1;

		auto pair2 = get(f2);
		unsigned int j = 1;

		unsigned int mergedFileSize = 0;

		while (i < firstSize && j < secondSize) {

			++mergedFileSize;

			if (pair1.first < pair2.first) {
				put(pair1, f3);
				pair1 = get(f1);
				++i;
			}

			else if (pair1.first > pair2.first) {
				put(pair2, f3);
				pair2 = get(f2);
				++j;
			}

			else {
				std::vector<unsigned long> mergedList;

				std::set_union(
					pair1.second.begin(), pair1.second.end(),
					pair2.second.begin(), pair2.second.end(),
					std::back_inserter(mergedList)
				);

				auto end = std::unique(mergedList.begin(), mergedList.end());

				mergedList.resize(end - mergedList.begin());

				auto combinedPair = std::make_pair(pair1.first, mergedList);

				put(combinedPair, f3);
				pair1 = get(f1);
				pair2 = get(f2);
				++i;
				++j;
			}
		}

		while (i++ < firstSize) {
			put(pair1, f3);
			pair1 = get(f1);
			mergedFileSize++;
		}

		while (j++ < secondSize) {
			put(pair2, f3);
			pair1 = get(f2);
			mergedFileSize++;
		}

		f3.write(reinterpret_cast<char*>(&mergedFileSize), sizeof(mergedFileSize));
	}
}

void BlockIndexer::put(const std::pair<unsigned long, std::vector<unsigned long>>& pair, std::ofstream& mergedFile) {

	mergedFile.write(reinterpret_cast<const char*>(&pair.first), sizeof(pair.first));

	unsigned int size = pair.second.size();

	mergedFile.write(reinterpret_cast<char*>(&size), sizeof(size));

	mergedFile.write(reinterpret_cast<const char*>(&pair.second[0]), sizeof(pair.second[0]) * size);
}

std::pair<unsigned long, std::vector<unsigned long>> BlockIndexer::get(std::ifstream& file) {

	unsigned long wordID = 0;

	file.read(reinterpret_cast<char*>(&wordID), sizeof(wordID));

	unsigned int size = 0;

	file.read(reinterpret_cast<char*>(&size), sizeof(size));

	std::vector<unsigned long> docIDs(size);

	file.read(reinterpret_cast<char*>(&docIDs[0]), sizeof(docIDs[0]) * size);

	return std::make_pair(wordID, docIDs);
}

int BlockIndexer::getIndexSizeFromFile(std::ifstream& file) {

	int sizeOfInt = sizeof(int);

	file.seekg(-sizeOfInt, std::ios::end);

	unsigned int size = 0;

	file.read(reinterpret_cast<char*>(&size), sizeof(size)); // read last for bytes to get size

	file.clear();

	file.seekg(0);

	return size;
}

void BlockIndexer::storeTermDict(std::string file) {

	std::ofstream out(outDir + file, std::ofstream::binary);

	if (out) {

		int size = termDict.size();

		out.write(reinterpret_cast<char*>(&size), sizeof(size));

		for (const auto& pair : termDict) {

			out.write(reinterpret_cast<const char*>(&pair.second), sizeof(pair.second));

			unsigned short size = pair.first.size();

			out.write(reinterpret_cast<char*>(&size), sizeof(size));

			out.write(pair.first.c_str(), size);
		}

	}
}

void BlockIndexer::storeDocDict(std::string file) {

	std::ofstream out(outDir + file, std::ofstream::binary);

	if (out) {

		int size = docDict.size();

		out.write(reinterpret_cast<char*>(&size), sizeof(size));

		for (const auto& pair : docDict) {

			out.write(reinterpret_cast<const char*>(&pair.second), sizeof(pair.second));

			short size = pair.first.size();

			out.write(reinterpret_cast<char*>(&size), sizeof(size));

			out.write(pair.first.c_str(), size);

		}

	}
}

std::unordered_map<unsigned long, std::vector<unsigned long>> BlockIndexer::readIndex(const std::string& filePath) {

	std::ifstream in(filePath, std::ifstream::binary | std::ifstream::ate); // open file with pointer to end

	if (!in) {
		return std::unordered_map<unsigned long, std::vector<unsigned long>>();
	}

	short sizeOfInt = sizeof(int);

	in.seekg(-sizeOfInt, std::ios::end); // point to start of last 4 bytes 

	int indexSize = 0;

	in.read(reinterpret_cast<char*>(&indexSize), sizeof(indexSize));

	in.clear();

	in.seekg(0);

	std::unordered_map<unsigned long, std::vector<unsigned long>> index;

	index.reserve(sizeOfInt);

	for (int i = 0; i < indexSize; i++) {

		unsigned long wordID = 0;

		in.read(reinterpret_cast<char*>(&wordID), sizeof(wordID));

		unsigned int listSize = 0;

		in.read(reinterpret_cast<char*>(&listSize), sizeof(listSize));

		std::vector<unsigned long> docIDs(listSize);

		in.read(reinterpret_cast<char*>(&docIDs[0]), sizeof(docIDs[0]) * listSize);

		index.emplace(wordID, docIDs);
	}

	return index;
}