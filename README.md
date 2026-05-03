# Weather Dashboard

A responsive weather dashboard that fetches real-time weather data from OpenWeatherMap API. Built with Flask backend and HTML/CSS/JavaScript frontend.

---

## ✨ Features

- 🌍 **Real-time Weather Data** - Fetch current weather conditions
- 📍 **City Search** - Search weather by city name
- 🌡️ **Temperature Display** - Current temp, "feels like", min/max
- 💨 **Weather Details** - Wind speed, humidity, pressure, visibility
- 🎨 **Responsive Design** - Works on desktop, tablet, and mobile
- 📊 **5-day Forecast** - Extended weather prediction
- 🌙 **Dark Mode** - Eye-friendly dark theme
- 📱 **Geolocation** - Auto-detect user's location
- 🔄 **Auto-Refresh** - Update weather every 10 minutes

---

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- pip (Python package manager)
- Git

### Installation Steps

1. **Clone the repository**
   ```bash
   git clone https://github.com/prateeksrivastava45/smartvaluation.git
   cd smartvaluation
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   ```

3. **Activate virtual environment**
   - **Windows:** `venv\Scripts\activate`
   - **macOS/Linux:** `source venv/bin/activate`

4. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

5. **Get Free API Key**
   - Visit [OpenWeatherMap](https://openweathermap.org/api)
   - Sign up for free account
   - Get your API key

6. **Create .env file**
   ```
   OPENWEATHER_API_KEY=your_api_key_here
   FLASK_ENV=development
   FLASK_DEBUG=True
   ```

7. **Run the application**
   ```bash
   python app.py
   ```

8. **Open in browser**
   - Go to: http://localhost:5000

---

## 📁 Project Structure

```
smartvaluation/
├── app.py                    # Flask application
├── requirements.txt          # Python dependencies
├── .env                      # Environment variables (create this)
├── .gitignore               # Git ignore rules
├── README.md                # This file
├── LICENSE                  # MIT License
├── SECURITY.md              # Security policy
│
├── static/                  # Static files
│   ├── css/
│   │   └── style.css        # Dashboard styling
│   └── js/
│       └── script.js        # Dashboard functionality
│
├── templates/
│   └── index.html           # Main dashboard HTML
│
└── src/                     # Source code
    ├── weather_api.py       # OpenWeatherMap integration
    ├── utils.py             # Utility functions
    └── config.py            # Flask configuration
```

---

## 🔌 API Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/` | GET | Main dashboard |
| `/api/weather` | GET | Get current weather |
| `/api/forecast` | GET | Get 5-day forecast |
| `/api/weather/coordinates` | GET | Get weather by lat/lon |
| `/health` | GET | Health check |

**Example API Call:**
```javascript
fetch('/api/weather?city=Delhi&units=metric')
  .then(r => r.json())
  .then(data => console.log(data))
```

---

## 🛠️ Technologies Used

- **Backend:** Flask, Python
- **Frontend:** HTML5, CSS3, JavaScript ES6+
- **API:** OpenWeatherMap
- **Libraries:** Requests, python-dotenv

---

## 🔐 Security

- ✅ API key stored in `.env` (never commit to git)
- ✅ Input validation on all searches
- ✅ CORS headers configured
- ✅ Environment-based configuration

See [SECURITY.md](SECURITY.md) for details.

---

## 🤝 Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/your-feature`
3. Commit changes: `git commit -m "Add feature"`
4. Push to branch: `git push origin feature/your-feature`
5. Open a Pull Request

---

## 📄 License

MIT License - See [LICENSE](LICENSE) file for details.

---

## 📧 Contact

- **Author:** Prateek Srivastava
- **GitHub:** [@prateeksrivastava45](https://github.com/prateeksrivastava45)

---

**⭐ Star this project if you find it helpful!**
