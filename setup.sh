#!/bin/bash
# Setup script for Road Safety Events Data Pipeline

set -e

echo "🚀 Setting up Road Safety Events Data Pipeline..."

# Create virtual environment
if [ ! -d ".venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv .venv
    echo "✓ Virtual environment created"
else
    echo "✓ Virtual environment already exists"
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source .venv/bin/activate

# Upgrade pip
echo "⬆️  Upgrading pip..."
pip install --upgrade pip

# Install dependencies
if [ -f "requirements.txt" ]; then
    echo "📥 Installing Python dependencies..."
    pip install -r requirements.txt
    echo "✓ Dependencies installed"
else
    echo "⚠️  requirements.txt not found, skipping dependency installation"
fi

echo ""
echo "✅ Setup complete!"
echo ""
echo "To activate the virtual environment in the future, run:"
echo "  source .venv/bin/activate"
echo ""
echo "To run the Streamlit app:"
echo "  export DATABASE_URL='your-connection-string'"
echo "  streamlit run scripts/streamlit_app.py"
echo ""
