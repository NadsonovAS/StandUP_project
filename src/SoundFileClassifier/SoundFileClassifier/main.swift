import Foundation
import SoundAnalysis

let arguments = CommandLine.arguments

guard arguments.count == 6 else {
    print("Usage: SoundFileClassifier <input_audio_path.wav> <output_json_path.json> <window_duration_seconds> <preferred_timescale> <confidence_threshold>")
    exit(1)
}

let inputAudioPath = arguments[1]
let outputJsonPath = arguments[2]

guard let windowDurationSeconds = Double(arguments[3]),
      let preferredTimescale = Int32(arguments[4]),
      let confidenceThreshold = Double(arguments[5]) else {
    print("Ошибка: не удалось преобразовать аргументы.")
    exit(1)
}

let version1 = SNClassifierIdentifier.version1
let classifySoundRequest = try SNClassifySoundRequest(classifierIdentifier: version1)

classifySoundRequest.windowDuration = CMTime(seconds: windowDurationSeconds, preferredTimescale: preferredTimescale)

let resultsObserver = ResultsObserver(confidenceThreshold: confidenceThreshold)

class ResultsObserver: NSObject, SNResultsObserving {
    let confidenceThreshold: Double
    var results: [Double: [(label: String, confidence: Double)]] = [:]
    init(confidenceThreshold: Double) {
        self.confidenceThreshold = confidenceThreshold
    }

    func request(_ request: SNRequest, didProduce result: SNResult) {

        guard let result = result as? SNClassificationResult else  { return }

        let topClassifications = result.classifications
            .filter { $0.confidence >= confidenceThreshold }
            .sorted { $0.confidence > $1.confidence }
            .prefix(10)
            .map { ($0.identifier, $0.confidence) }

        self.results[result.timeRange.start.seconds] = topClassifications

    }



    func request(_ request: SNRequest, didFailWithError error: Error) {
        print("The analysis failed: \(error.localizedDescription)")
    }


    func requestDidComplete(_ request: SNRequest) {
        print("The request completed successfully!")
    }
}


func createAnalyzer(audioFileURL: URL) -> SNAudioFileAnalyzer? {
    return try? SNAudioFileAnalyzer(url: audioFileURL)
}

let audioFileURL = URL(fileURLWithPath: inputAudioPath)

guard let audioFileAnalyzer = createAnalyzer(audioFileURL: audioFileURL) else {
    fatalError("Не удалось создать анализатор аудиофайла.")
}

try audioFileAnalyzer.add(classifySoundRequest, withObserver: resultsObserver)

audioFileAnalyzer.analyze()


let sortedLaughterResults: [(Double, Double)] = resultsObserver.results
    .compactMap { time, entries in
        guard let conf = entries.first(where: { $0.label == "laughter" })?.confidence else { return nil }
        return (time, conf)
    }
    .sorted { $0.0 < $1.0 }

var jsonString = "{\n"
for (time, conf) in sortedLaughterResults {
    jsonString += "  \"\(time)\": \(conf),\n"
}
if !sortedLaughterResults.isEmpty {
    jsonString.removeLast(2)
    jsonString += "\n"
}
jsonString += "}"

let outputURL = URL(fileURLWithPath: outputJsonPath)
if let data = jsonString.data(using: .utf8) {
    try? data.write(to: outputURL)
    print("Результаты анализа сохранены в: \(outputURL.path)")
} else {
    print("Ошибка сериализации JSON.")
}
 
