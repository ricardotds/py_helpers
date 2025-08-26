# Brazilian Debenture Management System - Deployment Package

## 📋 Overview

This is the complete deployment package for the Brazilian Debenture Management System - a comprehensive application for managing web crawlers that retrieve PDF files from Brazilian Fiduciary Agents, with AI-powered document analysis using LangChain.

## 🎯 Key Features

- **Web Crawlers**: Individual crawlers for 7+ Brazilian fiduciary agents
- **PDF Analysis**: LangChain integration with OpenAI GPT-4 for document analysis
- **Structured Outputs**: Customizable schemas for analysis results
- **CRUD Operations**: Complete management of prompts and schemas
- **Professional UI**: React TypeScript frontend with Material UI
- **REST API**: FastAPI backend with comprehensive documentation

## 🏗️ System Architecture

- **Backend**: FastAPI with SQLAlchemy, LangChain, and OpenAI integration
- **Frontend**: React TypeScript with Material UI components
- **Database**: SQLite (development) / PostgreSQL (production ready)
- **AI Integration**: OpenAI GPT-4 Turbo for document analysis

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Node.js 18+
- pnpm (recommended) or npm
- OpenAI API Key

### 1. Backend Setup

```bash
cd brazilian_debenture_management_system

# Install Python dependencies
pip install -r requirements_enhanced.txt

# Set up environment variables
cp .env.example .env
# Edit .env and add your OpenAI API key:
# OPENAI_API_KEY=your_openai_api_key_here

# Initialize database
python run.py --init-db

# Start backend server
python run.py --no-init
```

The backend will be available at: http://localhost:8000
API Documentation: http://localhost:8000/docs

### 2. Frontend Setup

```bash
cd frontend

# Install dependencies
pnpm install
# or: npm install

# Start development server
pnpm run dev --host
# or: npm run dev -- --host
```

The frontend will be available at: http://localhost:5173

## 🔧 Configuration

### Environment Variables (.env)

```env
# OpenAI Configuration (Required)
OPENAI_API_KEY=your_openai_api_key_here
OPENAI_API_BASE=https://api.openai.com/v1

# Database Configuration
DATABASE_URL=sqlite:///./debenture_management.db

# Security
SECRET_KEY=your_secret_key_here
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30

# Application Settings
MAX_FILE_SIZE=104857600  # 100MB
ALLOWED_EXTENSIONS=.pdf,.doc,.docx,.txt
```

## 📊 Application Features

### 1. Dashboard
- System overview with statistics
- Real-time metrics and charts
- System health monitoring

### 2. Crawler Management
- Individual control for each fiduciary agent
- Start/stop crawler operations
- Progress monitoring and logging

### 3. Document Management
- Document listing with search and filters
- File download functionality
- Document processing status

### 4. AI Analysis
- PDF analysis with LangChain integration
- Customizable system prompts
- Structured output schemas
- Real-time analysis progress

### 5. Prompt Management (CRUD)
- Create, read, update, delete system prompts
- Prompt categorization and versioning
- Test prompt functionality

### 6. Schema Management (CRUD)
- Create, read, update, delete output schemas
- Schema validation and examples
- Version control for schemas

## 🌐 API Endpoints

The system provides comprehensive REST API endpoints:

- **Crawlers**: `/api/v1/crawlers/*` - Crawler management
- **Documents**: `/api/v1/documents/*` - Document operations
- **Analysis**: `/api/v1/analysis/*` - AI analysis operations
- **Prompts**: `/api/v1/prompts/*` - System prompt CRUD
- **Schemas**: `/api/v1/schemas/*` - Output schema CRUD
- **Dashboard**: `/api/v1/dashboard/*` - Dashboard data

Full API documentation available at: http://localhost:8000/docs

## 🏢 Supported Fiduciary Agents

1. **Pentágono S.A. DTVM**
2. **Vórtx DTVM Ltda.**
3. **Oliveira Trust DTVM S.A.**
4. **Planner Trustee DTVM Ltda**
5. **BTG Pactual Serviços Financeiros S.A. DTVM**
6. **BRL Trust DTVM S.A.**
7. **XP Investimentos CCTVM S.A.**
8. **Banco do Brasil S.A.** (extensible)
9. **Itaú DTVM S.A.** (extensible)

## 🔒 Security Features

- JWT-based authentication
- Input validation and sanitization
- Rate limiting protection
- CORS configuration
- Secure file handling

## 📦 Production Deployment

### Docker Deployment

```bash
# Build and run with Docker Compose
docker-compose up -d

# Or for production
docker-compose -f docker-compose.prod.yml up -d
```

### Manual Production Setup

1. Set up PostgreSQL database
2. Configure environment variables for production
3. Use a production WSGI server (Gunicorn)
4. Set up reverse proxy (Nginx)
5. Configure SSL certificates

## 🛠️ Development

### Project Structure

```
brazilian_debenture_management_system/
├── app/                          # Backend application
│   ├── api/                      # API endpoints
│   ├── core/                     # Core utilities
│   ├── models/                   # Database models
│   ├── services/                 # Business logic
│   ├── crawlers/                 # Crawler implementations
│   └── schemas/                  # Pydantic schemas
├── frontend/                     # React frontend
│   ├── src/
│   │   ├── components/           # React components
│   │   └── App.jsx              # Main application
│   └── package.json
├── requirements_enhanced.txt     # Python dependencies
├── run.py                       # Application runner
└── README.md                    # Project documentation
```

### Adding New Crawlers

1. Create new crawler class in `app/crawlers/`
2. Inherit from `BaseCrawler`
3. Implement required methods
4. Add to crawler registry

### Customizing Analysis

1. Create new prompts via the web interface
2. Define custom output schemas
3. Configure analysis parameters
4. Run analysis on documents

## 🐛 Troubleshooting

### Common Issues

1. **OpenAI API Key Error**: Ensure OPENAI_API_KEY is set in .env
2. **Database Connection**: Check DATABASE_URL configuration
3. **Port Conflicts**: Ensure ports 8000 and 5173 are available
4. **Dependencies**: Run `pip install -r requirements_enhanced.txt`

### Logs and Debugging

- Backend logs: Check console output when running `python run.py`
- Frontend logs: Check browser developer console
- API testing: Use the interactive docs at `/docs`

## 📞 Support

For issues and questions:
- Check the API documentation at `/docs`
- Review the application logs
- Verify environment configuration

## 📄 License

MIT License - See LICENSE file for details

---

**Brazilian Debenture Management System v1.0**
*Complete AI-powered document management solution for Brazilian financial markets*

