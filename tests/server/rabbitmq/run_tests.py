import sys
import os
import subprocess
import argparse

project_root = os.path.join(os.path.dirname(__file__), '..', '..', '..')
sys.path.insert(0, project_root)

def run_command(command, description):
    """Run a command and print the results"""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {command}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            capture_output=True, 
            text=True,
            cwd=os.path.dirname(__file__)
        )
        
        print("STDOUT:")
        print(result.stdout)
        
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        
        print(f"Return code: {result.returncode}")
        return result.returncode == 0
        
    except Exception as e:
        print(f"Error running command: {e}")
        return False


def run_unit_tests():
    """Run unit tests"""
    command = "python -m pytest test_middleware_unit.py -v"
    return run_command(command, "Unit Tests for MessageMiddleware")


def run_integration_tests():
    """Run integration tests"""
    command = "python -m pytest test_middleware_integration.py -v"
    return run_command(command, "Integration Tests for Communication Patterns")


def run_all_tests():
    """Run all tests"""
    command = "python -m pytest . -v"
    return run_command(command, "All MessageMiddleware Tests")


def main():
    """Main test runner function"""
    parser = argparse.ArgumentParser(description='Run RabbitMQ Middleware Tests')
    parser.add_argument('--unit', action='store_true', help='Run only unit tests')
    parser.add_argument('--integration', action='store_true', help='Run only integration tests')
    parser.add_argument('--all', action='store_true', help='Run all tests')
    
    args = parser.parse_args()
    
    print("RabbitMQ Middleware Test Suite")
    print("=" * 60)
    print("Testing the following communication patterns:")
    print("1. Working Queue 1:1 - Single producer to single consumer")
    print("2. Working Queue 1:N - Single producer to multiple consumers")
    print("3. Exchange 1:1 - Single publisher to single subscriber")
    print("4. Exchange 1:N - Single publisher to multiple subscribers")
    print("=" * 60)
    
    success = True
    
    if args.unit:
        success &= run_unit_tests()
    elif args.integration:
        success &= run_integration_tests()
    elif args.all:
        success &= run_all_tests()
    else:
        success &= run_all_tests()
    
    print(f"\n{'='*60}")
    if success:
        print("ALL TESTS PASSED!")
        print("\n === Test Coverage Summary: ===")
        print("-> Working Queue 1:1 - Tested")
        print("-> Working Queue 1:N - Tested") 
        print("-> Exchange 1:1 - Tested")
        print("-> Exchange 1:N - Tested")
        print("-> Error handling - Tested")
        print("-> Connection management - Tested")
        print("-> Message acknowledgment - Tested")
    else:
        print("TESTS FAILED!")
    print(f"{'='*60}")
    
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
