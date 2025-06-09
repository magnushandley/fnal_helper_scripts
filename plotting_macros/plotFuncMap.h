// plotFuncMap.h
// Contains a map of function names to function pointers
// Allows for dynamic function calls based on string names

#pragma once

#include <functional>
#include <map>
#include <string>
#include "TTree.h"
#include "TH1D.h"

using FuncSig = std::function<TH1D*(TTree*, std::string, int, double, double)>;

TH1D* basicHist(TTree* tree, std::string varName, int bins, double xLow, double xHigh);
TH1D* sumEntryHist(TTree* tree, std::string varName, int bins, double xLow, double xHigh);

inline const std::map<std::string, FuncSig>& getFunctionMap() {
    static const std::map<std::string, FuncSig> funcMap {
        { "basicHist",      basicHist  },
        { "sumEntryHist", sumEntryHist }
    };
    return funcMap;
}