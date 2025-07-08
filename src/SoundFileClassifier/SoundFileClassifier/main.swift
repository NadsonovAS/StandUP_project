//
//  main.swift
//  SoundFileClassifier
//
//  Created by Aleksandr on 2025-07-06.
//

import Foundation
import SoundAnalysis

// Аргументы: [0] = executable name, [1] = path to audio file, [2] = output JSON file
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

/// An observer that receives results from a classify sound request.
class ResultsObserver: NSObject, SNResultsObserving {
    let confidenceThreshold: Double
    var results: [String: Double] = [:]
    init(confidenceThreshold: Double) {
        self.confidenceThreshold = confidenceThreshold
    }
    /// Notifies the observer when a request generates a prediction.
    func request(_ request: SNRequest, didProduce result: SNResult) {
        // Downcast the result to a classification result.
        guard let result = result as? SNClassificationResult else  { return }

        if let laughter = result.classifications.first(where: { $0.identifier == "laughter" && $0.confidence > self.confidenceThreshold }) {
            self.results[String(result.timeRange.start.seconds)] = laughter.confidence
        }

    }



    /// Notifies the observer when a request generates an error.
    func request(_ request: SNRequest, didFailWithError error: Error) {
        print("The analysis failed: \(error.localizedDescription)")
    }


    /// Notifies the observer when a request is complete.
    func requestDidComplete(_ request: SNRequest) {
        print("The request completed successfully!")
    }
}

/// Creates an analyzer for an audio file.
/// - Parameter audioFileURL: The URL to an audio file.
func createAnalyzer(audioFileURL: URL) -> SNAudioFileAnalyzer? {
    return try? SNAudioFileAnalyzer(url: audioFileURL)
}

// 🔽 Подставьте путь до вашего .wav файла
let audioFileURL = URL(fileURLWithPath: inputAudioPath)

guard let audioFileAnalyzer = createAnalyzer(audioFileURL: audioFileURL) else {
    fatalError("Не удалось создать анализатор аудиофайла.")
}

try audioFileAnalyzer.add(classifySoundRequest, withObserver: resultsObserver)

audioFileAnalyzer.analyze()

let sortedResults = resultsObserver.results.sorted { Double($0.key)! < Double($1.key)! }
let sortedDict = Dictionary(uniqueKeysWithValues: sortedResults.map { (key, value) in (String(key), value) })

let outputURL = URL(fileURLWithPath: outputJsonPath)
if let jsonData = try? JSONSerialization.data(withJSONObject: sortedDict, options: .prettyPrinted) {
    try? jsonData.write(to: outputURL)
    print("Результаты анализа сохранены в: \(outputURL.path)")
} else {
    print("Ошибка сериализации JSON.")
}
 
