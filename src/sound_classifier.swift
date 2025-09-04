import Foundation
import SoundAnalysis

/// Dictionary for compact JSON format: timestamp -> confidence
typealias LaughterResults = [String: Double]

// MARK: - Argument Parsing
struct Arguments {
    let inputAudioPath: String
    let windowDurationSeconds: Double
    let preferredTimescale: Int32
    let confidenceThreshold: Double
    let overlapFactor: Double
    
    init() throws {
        let args = CommandLine.arguments
        guard args.count == 6 else {
            throw ArgumentError.invalidCount
        }
        
        inputAudioPath = args[1]
        
        guard
            let windowDuration = Double(args[2]),
            let timescale = Int32(args[3]),
            let threshold = Double(args[4]),
            let overlap = Double(args[5])
        else {
            throw ArgumentError.invalidFormat
        }
        
        windowDurationSeconds = windowDuration
        preferredTimescale = timescale
        confidenceThreshold = threshold
        overlapFactor = overlap
    }
}

enum ArgumentError: Error {
    case invalidCount
    case invalidFormat
    
    var localizedDescription: String {
        switch self {
        case .invalidCount:
            return "Usage: SoundFileClassifier <input_audio_path.mp4> <window_duration_seconds> <preferred_timescale> <confidence_threshold> <overlap_factor>"
        case .invalidFormat:
            return "Error: Failed to parse arguments."
        }
    }
}

// MARK: - Laughter Detector
final class LaughterDetector: NSObject, SNResultsObserving {
    private let confidenceThreshold: Double
    private var laughterResults: [String: Double] = [:]
    private let laughterIdentifier = "laughter"
    
    init(confidenceThreshold: Double) {
        self.confidenceThreshold = confidenceThreshold
        super.init()
    }
    
    func request(_ request: SNRequest, didProduce result: SNResult) {
        guard let classificationResult = result as? SNClassificationResult else { return }
        
        if let laughterClassification = classificationResult.classifications.first(where: {
            $0.identifier == laughterIdentifier && $0.confidence >= confidenceThreshold
        }) {
            let timeKey = String(classificationResult.timeRange.start.seconds)
            let roundedConfidence = (round(laughterClassification.confidence * 100) / 100)
            laughterResults[timeKey] = roundedConfidence
        }
    }
    
    func getResults() -> LaughterResults {
        laughterResults
    }
}

// MARK: - Main Analysis
func runAnalysis() throws {
    let args = try Arguments()
    let audioFileURL = URL(fileURLWithPath: args.inputAudioPath)
    
    let classifySoundRequest = try SNClassifySoundRequest(classifierIdentifier: .version1)
    classifySoundRequest.windowDuration = CMTime(
        seconds: args.windowDurationSeconds,
        preferredTimescale: args.preferredTimescale
    )
    classifySoundRequest.overlapFactor = args.overlapFactor
    
    let laughterDetector = LaughterDetector(confidenceThreshold: args.confidenceThreshold)
    
    guard let audioFileAnalyzer = try? SNAudioFileAnalyzer(url: audioFileURL) else {
        throw NSError(
            domain: "AudioAnalysisError",
            code: 1,
            userInfo: [NSLocalizedDescriptionKey: "Failed to create audio file analyzer."]
        )
    }
    
    try audioFileAnalyzer.add(classifySoundRequest, withObserver: laughterDetector)
    audioFileAnalyzer.analyze()
    
    let results = laughterDetector.getResults()
    try saveResults(results)
}

// MARK: - Save Results
func saveResults(_ results: LaughterResults) throws {
    let sortedResults = results.sorted {
        (Double($0.key) ?? 0) < (Double($1.key) ?? 0)
    }
    
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
    
    let jsonData = try encoder.encode(Dictionary(uniqueKeysWithValues: sortedResults))
    if let jsonString = String(data: jsonData, encoding: .utf8) {
        print(jsonString)
    }
}

// MARK: - Entry Point
do {
    try runAnalysis()
} catch let error as ArgumentError {
    print(error.localizedDescription)
    exit(1)
} catch {
    print("Analysis error: \(error.localizedDescription)")
    exit(1)
}