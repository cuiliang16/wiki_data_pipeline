# Wikipedia Data Pipeline

A comprehensive Python pipeline for fetching Wikipedia articles via the MediaWiki API and storing them in Azure Blob Storage with JSON format.

## Features

- **Complete Wikipedia Data Fetching**: Uses MediaWiki API to fetch all Wikipedia articles
- **Azure Blob Storage Integration**: Stores articles in organized batches (1000 files per directory)
- **Robust Error Handling**: Retry logic with exponential backoff for failed requests
- **Rate Limiting**: Respects Wikipedia API rate limits (429 responses only)
- **Resumable Processing**: Checkpoint system allows resuming interrupted processing
- **Comprehensive Logging**: Detailed logs with separate error tracking
- **Configurable**: YAML configuration with environment variable support
- **Queue System**: Persistent queue for reliable article processing

## Requirements

- Python 3.7+
- Azure Blob Storage account
- Internet connection for Wikipedia API access

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd wiki_data
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure the application:
   - Copy `config/env_template.txt` to `config/.env`
   - Update the Azure connection string in `config/.env`
   - Modify `config/config.yaml` as needed

## Configuration

### Environment Variables (`config/.env`)
```
AZURE_CONNECTION_STRING=your_actual_connection_string
```

### Main Configuration (`config/config.yaml`)

```yaml
azure:
  connection_string: "your_connection_string"
  container_name: "wikipedia-data"
  batch_size: 1000

naming:
  file_pattern: "article_{index:06d}.json"
  directory_pattern: "batch_{batch_num:03d}"

api:
  base_url: "https://en.wikipedia.org/w/api.php"
  user_agent: "WikipediaDataPipeline/1.0"
  max_retries: 3
  initial_delay: 1
  max_delay: 60

logging:
  level: "INFO"
  main_log: "logs/wiki_pipeline.log"
  error_log: "logs/errors.log"
  max_file_size: 10485760  # 10MB
  backup_count: 5

processing:
  batch_size: 1000
  checkpoint_interval: 100
  queue_persistence: true
```

## Usage

### Basic Usage

Run the pipeline with default settings:
```bash
python src/main.py
```

### Advanced Usage

Limit the number of articles to process:
```bash
python src/main.py --limit 1000
```

Use a custom configuration file:
```bash
python src/main.py --config path/to/config.yaml
```

Reset checkpoint and start fresh:
```bash
python src/main.py --reset
```

### Command Line Options

- `--config`: Path to configuration file (default: `config/config.yaml`)
- `--limit`: Limit number of articles to process
- `--reset`: Reset checkpoint and queue, start fresh

## Data Structure

Each article is stored as a JSON file with the following structure:

```json
{
  "title": "Article Title",
  "url": "https://en.wikipedia.org/wiki/Article_Title",
  "content": "HTML content of the article",
  "categories": ["Category1", "Category2"],
  "author": null,
  "pageid": "12345",
  "metadata": {
    "content_length": 15000,
    "category_count": 5,
    "processing_timestamp": "2024-01-01T12:00:00"
  }
}
```

## File Organization

Articles are organized in Azure Blob Storage as follows:
```
container/
├── batch_001/
│   ├── article_000001.json
│   ├── article_000002.json
│   └── ...
├── batch_002/
│   ├── article_001001.json
│   ├── article_001002.json
│   └── ...
└── ...
```

## Logging

The pipeline provides comprehensive logging:

- **Main Log**: `logs/wiki_pipeline.log` - General processing information
- **Error Log**: `logs/errors.log` - Detailed error information
- **Console Output**: Real-time progress and status updates

Log levels:
- `INFO`: General processing information
- `ERROR`: Error messages and failures
- `DEBUG`: Detailed debugging information

## Error Handling

- **Retry Logic**: 3 attempts with exponential backoff for failed requests
- **Rate Limiting**: Automatic handling of 429 responses with delays
- **Failed Articles**: Tracked separately and logged
- **Resumable**: Can resume from last checkpoint after interruption

## Checkpoint System

The pipeline maintains checkpoints to enable resumption:

- **Progress Tracking**: Current article index and batch number
- **Statistics**: Total processed and failed articles
- **Timing**: Processing start time and duration
- **Status**: Current processing status (idle/processing/completed)

## Queue System

Persistent queue for reliable processing:

- **JSON Storage**: Queue state saved to `queue/article_queue.json`
- **Resume Support**: Can resume from last processed article
- **Status Tracking**: Processed, failed, and queued articles
- **Recovery**: Automatic recovery from interruptions

## Performance Considerations

- **Batch Processing**: Articles processed in configurable batches
- **Memory Efficient**: Streaming processing to avoid memory issues
- **Rate Limiting**: Respects Wikipedia API limits
- **Parallel Uploads**: Efficient Azure Blob Storage uploads

## Monitoring

Monitor the pipeline using:

1. **Log Files**: Check `logs/wiki_pipeline.log` for progress
2. **Error Log**: Review `logs/errors.log` for issues
3. **Checkpoint Status**: Check `checkpoints/progress.json`
4. **Queue Status**: Monitor `queue/article_queue.json`

## Troubleshooting

### Common Issues

1. **Azure Connection Failed**
   - Verify connection string in `config/.env`
   - Check Azure account permissions

2. **Wikipedia API Errors**
   - Check internet connection
   - Verify API endpoint accessibility
   - Review rate limiting settings

3. **Memory Issues**
   - Reduce batch size in configuration
   - Monitor system resources

4. **Processing Stuck**
   - Check logs for error messages
   - Verify checkpoint file integrity
   - Consider resetting with `--reset` flag

### Debug Mode

Enable debug logging by changing log level in config:
```yaml
logging:
  level: "DEBUG"
```

## Development

### Project Structure
```
wiki_data/
├── config/
│   ├── config.yaml
│   └── env_template.txt
├── src/
│   ├── __init__.py
│   ├── config_manager.py
│   ├── logging_manager.py
│   ├── queue_manager.py
│   ├── mediawiki_client.py
│   ├── azure_manager.py
│   ├── data_processor.py
│   ├── checkpoint_manager.py
│   └── main.py
├── logs/
├── queue/
├── checkpoints/
├── requirements.txt
└── README.md
```

### Adding New Features

1. **New Configuration**: Add to `config.yaml` and `ConfigManager`
2. **New Components**: Create new modules in `src/`
3. **New Logging**: Use `LoggingManager` for consistent logging
4. **Testing**: Test with small limits first

## License

[Add your license information here]

## Contributing

[Add contribution guidelines here] 