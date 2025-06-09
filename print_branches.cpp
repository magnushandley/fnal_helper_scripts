#include <TFile.h>
#include <TTree.h>
#include <TBranch.h>
#include <TObjArray.h>
#include <iostream>

// listBranches.C
void listBranches(const char *filename = "myfile.root", const char *treename = "treeName") {
  // Open the ROOT file
  TFile *file = TFile::Open(filename);
  if (!file || file->IsZombie()) {
    std::cerr << "Error: Cannot open file " << filename << std::endl;
    return;
  }
    
  // Retrieve the TTree from the file
  TTree *tree = (TTree*)file->Get(treename);
  if (!tree) {
    std::cerr << "Error: Tree " << treename << " not found in file " << filename << std::endl;
    file->Close();
    return;
  }
    
  // Get the list of branches in the tree
  TObjArray *branches = tree->GetListOfBranches();
  std::cout << "Branches in tree '" << treename << "':" << std::endl;
    
  // Loop over the branches and output each branch name
  for (int i = 0; i < branches->GetEntries(); ++i) {
    TBranch *branch = (TBranch*)branches->At(i);
    std::cout << branch->GetName() << std::endl;
  }
    
  // Clean up: close the file
  file->Close();
}

int main(int argc, char **argv) {
  // Allow passing filename and treename as command-line arguments
  const char *filename = (argc > 1) ? argv[1] : "myfile.root";
  const char *treename = (argc > 2) ? argv[2] : "treeName";

  listBranches(filename, treename);

  return 0;
}
