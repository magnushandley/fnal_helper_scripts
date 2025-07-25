#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <utility> 
#include <iomanip>
#include <map>
#include "TFile.h"
#include "TTree.h"
#include "TH1D.h"  
#include "TF1.h"
#include "TCanvas.h"
#include "TROOT.h"
#include "TStyle.h"
#include "TLine.h"
#include "TLatex.h"
#include "plotFuncMap.h"

void plotHist(TH1D *hist, std::string xAxis, std::string yAxis, std::string title,  std::string saveFile, double minVal = -999, double maxVal = -999)   {
  
  TCanvas *canvas = new TCanvas("canvas", "Cross Correlation", 800, 600);

  //Only bother setting the range if we've given it a value
  if (minVal != -999) {
    hist->GetXaxis()->SetRangeUser(minVal, maxVal);
  }

  hist->GetXaxis()->SetTitle(xAxis.c_str()); 
  hist->GetYaxis()->SetTitle(yAxis.c_str());    
  hist->SetLineWidth(2);
  hist->SetLineColor(kRed);
  hist->Draw("HIST");
  hist->SetTitle(title.c_str());
  canvas->Update();
  canvas->SaveAs(saveFile.c_str());
  delete canvas;
  canvas = nullptr;
}

TH1D* basicHist(TTree* tree, std::string varName, int bins, double xLow, double xHigh) {
  TH1D* hist = new TH1D(varName.c_str(), "", bins, xLow, xHigh);
  tree->Project(hist->GetName(), varName.c_str());
  return hist;
}

TH1D* sumEntryHist(TTree* tree, std::string varName, int bins, double xLow, double xHigh) {
  // Rather than loading a basic histogram, this sums the number of values in a vector entry, e.g. number of entries in an array of hits vs time

  TString histName = TString::Format("sum_%s", varName.c_str());
  TH1D* hist = new TH1D(histName, "", bins, xLow, xHigh);
  std::vector<double>* vec = nullptr; 
  tree->SetBranchAddress(varName.c_str(), &vec);

  //Requires looping over entries rather than just projecting the histogram

  Long64_t nEntries = tree->GetEntries();
  for (Long64_t i = 0; i < nEntries; ++i) {
    tree->GetEntry(i);          // loads “*vec” for this entry
    if (vec) {
      hist->Fill(static_cast<double>(vec->size()));
    }
  }

  return hist;
}

int main(int argc, char* argv[]) {
  //Takes a file and a series of variables to plot, along with titles, ranges and axis labels
  //Format of file: varName xLow xHigh bins xAxis yAxis title saveString
  
  if (argc<3) {
    std::cerr<<"Usage: "<<argv[0]<<" <input.root> <config.txt>\n";
    return 1;
  }
  
  //Style
  gStyle->SetTextFont(132);
  gStyle->SetLabelFont(132, "XYZ");   // Set for X, Y, and Z axes
  gStyle->SetTitleFont(132, "XYZ");   // Set for axis titles
  gStyle->SetTitleFont(132, "Title");

  //Load command line arguments
  std::string dataPath = argv[1];
  std::string configPath = argv[2];
  std::string branch = argv[3];

  std::ifstream cfg(configPath);
  
  if (!cfg) {
    std::cerr << "Error: could not open config file " << configPath << "\n";
    return 1;
  }

  std::string line;

  //Read ROOT file
  
  TFile* f = TFile::Open(dataPath.c_str());

  if (!f || f->IsZombie()) {
    std::cerr << "Could not open input ROOT file\n";
    return 1;
  }
  
  //Loop through config file lines to plot the different variables

  while (std::getline(cfg, line)) {
    // skip blank lines or comments
    if (line.empty() || line[0] == '#') continue;

    std::istringstream iss(line);
    std::string varName;
    double xLow, xHigh;
    int bins;
    std::string xAxis, yAxis, title, outFile;
    std::string plotType; 

    // parse the mandatory fields
    if (!(iss >> std::quoted(varName) >> xLow >> xHigh >> bins
          >> std::quoted(xAxis) >> std::quoted(yAxis)
          >> std::quoted(title) >> std::quoted(outFile)
          >> std::quoted(plotType))) 
        {
    std::cerr << "Malformed line: " << line << "\n";
    continue;
        }

    // fetch the histogram by name:
    TTree* tree = dynamic_cast<TTree*>(f->Get(branch.c_str()));

    auto const& table = getFunctionMap();
    auto funcEntry = table.find(plotType);
    TH1D* hist = funcEntry->second(tree, varName, bins, xLow, xHigh);
    

    // call plotting helper
    plotHist(hist, xAxis, yAxis, title, outFile, xLow, xHigh);
  }
  f->Close();
  delete f;
  f = nullptr;
  return 0;
}
  
  
  
  

  
