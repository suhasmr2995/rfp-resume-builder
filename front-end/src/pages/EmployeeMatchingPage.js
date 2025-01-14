import React, { useState } from 'react';
import { Users } from 'lucide-react';
import RFPSelector from '../components/rfp/RFPSelector';
import SearchControls from '../components/EmployeeMatching/SearchControls';
import ResultsTable from '../components/EmployeeMatching/ResultsTable';
import ActionButtons from '../components/EmployeeMatching/ActionButtons';
import EnhancedResumes from '../components/EmployeeMatching/EnhancedResumes';
import ResumeViewer from '../components/EmployeeMatching/ResumeViewer';

const EmployeeMatchingPage = () => {
  const [selectedRFP, setSelectedRFP] = useState(null);
  const [matchingResults, setMatchingResults] = useState([]);
  const [refineSearch, setRefineSearch] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [selectedRows, setSelectedRows] = useState([]);
  const [expandedRows, setExpandedRows] = useState({});
  const [enhancedResults, setEnhancedResults] = useState([]);
  const [isWindowOpen, setIsWindowOpen] = useState(false);
  const [viewingResume, setViewingResume] = useState(null);
  const [selectedEnhancedResumes, setSelectedEnhancedResumes] = useState([]);

  const handleRunMatching = async () => {
    if (!selectedRFP) {
      alert("Please select an RFP before running the matching process.");
      return;
    }
    setIsLoading(true);
    try {
      const response = await fetch('http://localhost:5000/search', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          rfpName: selectedRFP,
          feedback: refineSearch,
        }),
      });
      const data = await response.json();
      if (response.ok) {
        setMatchingResults(data.results.map(result => ({
          ...result
        })));
      } else {
        alert(data.error || 'An error occurred during the search');
      }
    } catch (error) {
      console.error('Error during search:', error);
      alert('An error occurred during the search');
    } finally {
      setIsLoading(false);
    }
  };

  const handleResumeClick = (resumeName) => {
    if (!selectedRFP) {
      alert("Please select an RFP before viewing resumes.");
      return;
    }
    setViewingResume({ name: resumeName });
    setIsWindowOpen(true);
  };

  const handleEnhanceResumes = async () => {
    document.body.style.cursor = 'progress';
    const enhancedResults = [];

    for (const resumeName of selectedRows) {
      try {
        const search_query = matchingResults.find(x => x.name === resumeName)?.search_query ?? '';
        const response = await fetch('http://localhost:5000/enhance', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            resumeName: resumeName,
            rfpName: selectedRFP,
            search_query: search_query
          }),
        });

        if (!response.ok) {
          throw new Error('Network response was not ok');
        }

        const data = await response.json();
        enhancedResults.push({
          name: resumeName,
          enhancedResumeName: data.enhancedResumeName,
        });
      } catch (error) {
        console.error(`Error enhancing resume ${resumeName}:`, error);
        // Optionally, you can add error handling UI here
      }
      finally {
     document.body.style.cursor = 'default'
    }

    setEnhancedResults(enhancedResults);
    setIsLoading(false);
    };
  };

  const handleDownloadResumes = async () => {
    const resumesToDownload = [...selectedRows, ...selectedEnhancedResumes];
    for (const resumeName of resumesToDownload) {
      setIsLoading(true);
      try {
        const url = `http://localhost:5000/download?resumeName=${resumeName}`;
        
        const response = await fetch(url, {
          method: 'GET',
          headers: {
            'Access-Control-Allow-Origin': 'http://localhost:3001',
          },
        });
  
        if (!response.ok) {
          throw new Error('Network response was not ok');
        }
  
        const blob = await response.blob();
        const downloadUrl = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = downloadUrl;
        a.download = resumeName;
        document.body.appendChild(a);
        a.click();
        a.remove();
      } catch (error) {
        console.error('Error downloading document:', error);
        alert(`An error occurred while downloading ${resumeName}`);
      } finally {
        setIsLoading(false);
      }
    }
  };

  const handleEnhancedResumeSelect = (resumeName) => {
    setSelectedEnhancedResumes(prev =>
      prev.includes(resumeName)
        ? prev.filter(name => name !== resumeName)
        : [...prev, resumeName]
    );
  };

  return (
    <div className="flex flex-col h-full pt-4">
      <div className="text-center mb-8 pb-2">
        <div className="flex justify-center items-center mb-2">
          <Users className="text-blue-400 mr-2" size={36} />
          <h1 className="text-4xl font-bold leading-tight text-transparent bg-clip-text bg-gradient-to-r from-blue-400 to-purple-500">
            Resume Builder
          </h1>
        </div>
        <span className="px-3 py-1 inline-flex text-sm leading-5 font-medium rounded-full bg-gradient-to-r from-blue-600 to-green text-white">
          Find the right people for your proposal
        </span>
      </div>
      <div className="flex flex-1 px-4 pb-16">
        <div className="w-64 pr-4 flex flex-col space-y-4">
        <RFPSelector
            selectedRFPs={selectedRFP}
            onSelectRFP={setSelectedRFP}
            multiSelect={false}
          />
          <SearchControls
            onRunMatching={handleRunMatching}
            refineSearch={refineSearch}
            setRefineSearch={setRefineSearch}
          />
        </div>

        <div className="flex-1 px-4 flex flex-col">
          <ResultsTable
            isLoading={isLoading}
            matchingResults={matchingResults}
            selectedRows={selectedRows}
            setSelectedRows={setSelectedRows}
            expandedRows={expandedRows}
            setExpandedRows={setExpandedRows}
            onResumeClick={handleResumeClick}
          />

          <ActionButtons
            selectedRows={selectedRows}
            selectedEnhancedResumes={selectedEnhancedResumes}
            onEnhanceResumes={handleEnhanceResumes}
            onDownloadResumes={handleDownloadResumes}
          />

          <EnhancedResumes
            enhancedResults={enhancedResults}
            onResumeClick={handleResumeClick}
            selectedEnhancedResumes={selectedEnhancedResumes}
            onEnhancedResumeSelect={handleEnhancedResumeSelect}
          />

          <ResumeViewer
            resumeName={viewingResume?.name}
            isWindowOpen={isWindowOpen}
            onClose={() => {
              setIsWindowOpen(false);
              setViewingResume(null);
            }}
          />
        </div>
      </div>
    </div>
  );
};

export default EmployeeMatchingPage;