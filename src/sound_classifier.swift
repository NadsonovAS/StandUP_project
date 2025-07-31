import Foundation
import SoundAnalysis

// Используем словарь для более компактного JSON формата
typealias LaughterResults = [String: Double]

// MARK: - Парсинг аргументов
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
        
        guard let windowDuration = Double(args[2]),
              let timescale = Int32(args[3]),
              let threshold = Double(args[4]),
              let overlap = Double(args[5]) else {
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
            return "Ошибка: не удалось преобразовать аргументы."
        }
    }
}

// MARK: - Оптимизированный класс для обработки результатов
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
        
        // Ищем только смех, не сохраняем все классификации
        if let laughterClassification = classificationResult.classifications.first(where: { 
            $0.identifier == laughterIdentifier && $0.confidence >= confidenceThreshold 
        }) {
            let timeKey = String(classificationResult.timeRange.start.seconds)
            // Округляем confidence до 2 знаков после запятой
            let roundedConfidence = round(laughterClassification.confidence * 100) / 100
            laughterResults[timeKey] = roundedConfidence
        }
    }
    
    func getResults() -> LaughterResults {
        return laughterResults
    }
}

// MARK: - Основная функция
func analyzeLaughter() throws {
    let args = try Arguments()
    
    let audioFileURL = URL(fileURLWithPath: args.inputAudioPath)
    
    // Создаем запрос для анализа
    let classifySoundRequest = try SNClassifySoundRequest(classifierIdentifier: .version1)
    classifySoundRequest.windowDuration = CMTime(
        seconds: args.windowDurationSeconds,
        preferredTimescale: args.preferredTimescale
    )
    classifySoundRequest.overlapFactor = args.overlapFactor
    
    // Создаем детектор смеха
    let laughterDetector = LaughterDetector(confidenceThreshold: args.confidenceThreshold)
    
    // Создаем и настраиваем анализатор
    guard let audioFileAnalyzer = try? SNAudioFileAnalyzer(url: audioFileURL) else {
        throw NSError(domain: "AudioAnalysisError", code: 1, userInfo: [
            NSLocalizedDescriptionKey: "Не удалось создать анализатор аудиофайла."
        ])
    }
    
    try audioFileAnalyzer.add(classifySoundRequest, withObserver: laughterDetector)
    audioFileAnalyzer.analyze()
    
    // Получаем результаты
    let results = laughterDetector.getResults()
    
    // Сохраняем результаты
    try saveResults(results)
    
}

// MARK: - Сохранение результатов
func saveResults(_ results: LaughterResults) throws {
    // Сортируем результаты по времени (численно) перед сериализацией
    let sortedResults = results.sorted { (first, second) in
        let firstTime = Double(first.key) ?? 0
        let secondTime = Double(second.key) ?? 0
        return firstTime < secondTime
    }
    
    // Создаем JSON вручную для сохранения порядка
    let encoder = JSONEncoder()
    encoder.outputFormatting = .prettyPrinted
    
    var jsonString = "{\n"
    for (index, (key, value)) in sortedResults.enumerated() {
        let valueData = try encoder.encode(value)
        let valueString = String(data: valueData, encoding: .utf8)!
        jsonString += "  \"\(key)\" : \(valueString)"
        if index < sortedResults.count - 1 {
            jsonString += ","
        }
        jsonString += "\n"
    }
    jsonString += "}"
    
    print(jsonString)
}

// MARK: - Точка входа
do {
    try analyzeLaughter()
} catch let error as ArgumentError {
    print(error.localizedDescription)
    exit(1)
} catch {
    print("Ошибка при анализе: \(error.localizedDescription)")
    exit(1)
}